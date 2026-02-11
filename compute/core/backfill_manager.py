"""
Backfill manager for processing backfill jobs using DuckDB.

Manages backfill job queue and executes historical data replication.
"""

import logging
import threading
import time
from typing import List, Optional
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor

from core.database import get_connection_pool
from core.models import Source, QueueBackfillData, BackfillStatus
from core.security import decrypt_value
from config.config import get_config

try:
    import duckdb
except ImportError:
    duckdb = None
    logging.warning("DuckDB not installed. Backfill feature will not work.")

logger = logging.getLogger(__name__)


class BackfillManager:
    """
    Manages backfill job execution using DuckDB.

    Polls queue_backfill_data table for PENDING jobs and processes them
    using DuckDB's PostgreSQL scanner for efficient batch processing.
    """

    def __init__(self, check_interval: int = 5, batch_size: int = 10000):
        """
        Initialize backfill manager.

        Args:
            check_interval: Seconds between queue checks
            batch_size: Number of rows per batch
        """
        self.check_interval = check_interval
        self.batch_size = batch_size
        self.stop_event = threading.Event()
        self.active_jobs: dict[int, threading.Thread] = {}
        self.active_jobs_lock = threading.Lock()

        if not duckdb:
            logger.error("DuckDB is not installed. Install with: pip install duckdb")

    def start(self) -> None:
        """Start the backfill manager thread."""
        logger.info("Starting BackfillManager")
        monitor_thread = threading.Thread(target=self._monitor_queue, daemon=True)
        monitor_thread.start()
        logger.info("BackfillManager started")

    def stop(self) -> None:
        """Stop the backfill manager."""
        logger.info("Stopping BackfillManager")
        self.stop_event.set()

        # Wait for active jobs to complete (with timeout)
        with self.active_jobs_lock:
            for job_id, thread in list(self.active_jobs.items()):
                if thread.is_alive():
                    logger.info(f"Waiting for backfill job {job_id} to complete")
                    thread.join(timeout=30)

        logger.info("BackfillManager stopped")

    def _monitor_queue(self) -> None:
        """Monitor queue for pending backfill jobs."""
        logger.info("Backfill queue monitor started")

        while not self.stop_event.is_set():
            try:
                pending_jobs = self._get_pending_jobs()

                for job in pending_jobs:
                    # Check if we should stop
                    if self.stop_event.is_set():
                        break

                    # Skip if already processing
                    with self.active_jobs_lock:
                        if job["id"] in self.active_jobs:
                            continue

                    # Start job in background thread
                    logger.info(
                        f"Starting backfill job {job['id']} for table {job['table_name']}"
                    )
                    job_thread = threading.Thread(
                        target=self._execute_backfill_job,
                        args=(job,),
                        daemon=True,
                    )

                    with self.active_jobs_lock:
                        self.active_jobs[job["id"]] = job_thread

                    job_thread.start()

            except Exception as e:
                logger.error(f"Error in backfill queue monitor: {e}")

            # Sleep before next check
            time.sleep(self.check_interval)

        logger.info("Backfill queue monitor stopped")

    def _get_pending_jobs(self) -> List[dict]:
        """
        Get pending backfill jobs from database.

        Returns:
            List of pending job records
        """
        max_retries = 3
        retry_delay = 1

        for attempt in range(max_retries):
            pool = None
            conn = None
            try:
                pool = get_connection_pool()
                conn = pool.getconn()
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute(
                        """
                        SELECT qb.*, s.pg_host, s.pg_port, s.pg_database, 
                               s.pg_username, s.pg_password
                        FROM queue_backfill_data qb
                        JOIN sources s ON qb.source_id = s.id
                        WHERE qb.status = %s
                        ORDER BY qb.created_at ASC
                        LIMIT 10
                        """,
                        (BackfillStatus.PENDING.value,),
                    )
                    jobs = cursor.fetchall()
                    return [dict(job) for job in jobs]
            except Exception as e:
                logger.error(
                    f"Error fetching pending jobs (attempt {attempt + 1}/{max_retries}): {e}"
                )
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    retry_delay *= 2
            finally:
                if conn and pool:
                    try:
                        pool.putconn(conn)
                    except Exception:
                        pass

        return []

    def _execute_backfill_job(self, job: dict) -> None:
        """
        Execute a backfill job using DuckDB.

        Args:
            job: Job configuration dictionary
        """
        job_id = job["id"]

        try:
            # Update status to EXECUTING
            self._update_job_status(job_id, BackfillStatus.EXECUTING.value)

            # Check if DuckDB is available
            if not duckdb:
                raise RuntimeError("DuckDB is not installed")

            # Execute backfill
            total_records = self._process_backfill_with_duckdb(job)

            # Check if job was cancelled during processing
            if self._is_job_cancelled(job_id):
                logger.info(
                    f"Backfill job {job_id} was cancelled. Processed {total_records} records before cancellation."
                )
                # Status is already CANCELLED, just return
                return

            # Update to COMPLETED only if not cancelled
            self._update_job_status(
                job_id,
                BackfillStatus.COMPLETED.value,
                count_record=total_records,
            )
            logger.info(
                f"Backfill job {job_id} completed successfully. Total records: {total_records}"
            )

        except Exception as e:
            logger.error(f"Backfill job {job_id} failed: {e}")
            error_msg = str(e)[:500]  # Truncate long error messages
            self._update_job_status(
                job_id,
                BackfillStatus.FAILED.value,
                error_message=error_msg,
            )
        finally:
            # Remove from active jobs
            with self.active_jobs_lock:
                if job_id in self.active_jobs:
                    del self.active_jobs[job_id]

    def _process_backfill_with_duckdb(self, job: dict) -> int:
        """
        Process backfill using DuckDB PostgreSQL scanner.

        Args:
            job: Job configuration

        Returns:
            Total number of records processed
        """
        job_id = job["id"]
        table_name = job["table_name"]
        filter_sql = job.get("filter_sql")

        # Build PostgreSQL connection string
        pg_conn_str = self._build_postgres_connection(job)

        # Initialize DuckDB connection (in-memory)
        conn = duckdb.connect(":memory:")

        total_processed = 0

        try:
            # Install and load postgres_scanner extension
            conn.execute("INSTALL postgres_scanner")
            conn.execute("LOAD postgres_scanner")

            # Attach PostgreSQL database
            logger.info(f"Attaching to PostgreSQL: {job['pg_database']}")
            conn.execute(
                f"""
                ATTACH '{pg_conn_str}' AS source_db (TYPE POSTGRES)
                """
            )

            # Build SELECT query with optional filters
            base_query = f"SELECT * FROM source_db.{table_name}"

            if filter_sql:
                # Parse semicolon-separated filters and convert to WHERE clause
                where_clauses = filter_sql.split(";")
                where_clause = " AND ".join(
                    f"({clause.strip()})" for clause in where_clauses if clause.strip()
                )
                if where_clause:
                    base_query += f" WHERE {where_clause}"

            # Count total rows first
            count_query = f"SELECT COUNT(*) as total FROM ({base_query}) t"
            total_rows = conn.execute(count_query).fetchone()[0]
            logger.info(f"Job {job_id}: Total rows to process: {total_rows}")

            # Update total_record in database
            self._update_job_total_record(job_id, total_rows)

            # Process in batches
            offset = 0
            while not self.stop_event.is_set():
                # Check if job was cancelled
                if self._is_job_cancelled(job_id):
                    logger.info(f"Job {job_id} was cancelled, stopping processing")
                    break

                # Calculate dynamic batch size to not exceed total records
                remaining = total_rows - offset
                if remaining <= 0:
                    break

                current_batch_size = min(self.batch_size, remaining)
                batch_query = f"{base_query} LIMIT {current_batch_size} OFFSET {offset}"

                logger.debug(
                    f"Job {job_id}: Processing batch at offset {offset}, batch_size={current_batch_size}"
                )
                result = conn.execute(batch_query).fetchall()

                if not result:
                    break

                # Get column names
                columns = [desc[0] for desc in conn.description]

                # Process batch - convert to CDC events and send to destinations
                batch_records = [dict(zip(columns, row)) for row in result]
                self._process_batch_to_destinations(job, batch_records)

                # Update progress
                total_processed += len(batch_records)
                self._update_job_count(job_id, total_processed)

                logger.info(
                    f"Job {job_id}: Processed {total_processed}/{total_rows} records"
                )

                offset += len(batch_records)

            return total_processed

        finally:
            conn.close()

    def _build_postgres_connection(self, job: dict) -> str:
        """
        Build PostgreSQL connection string for DuckDB.

        Args:
            job: Job configuration with database details

        Returns:
            Connection string with decrypted password
        """
        # Decrypt password if encrypted
        password = decrypt_value(job["pg_password"] or "")

        return (
            f"dbname={job['pg_database']} "
            f"user={job['pg_username']} "
            f"password={password} "
            f"host={job['pg_host']} "
            f"port={job['pg_port']}"
        )

    def _process_batch_to_destinations(self, job: dict, records: List[dict]) -> None:
        """
        Process batch of records to destinations.

        Args:
            job: Job configuration
            records: Batch of records to process
        """
        from core.repository import (
            PipelineRepository,
            DestinationRepository,
            DataFlowRepository,
        )
        from core.models import DestinationType
        from destinations.base import CDCRecord
        from destinations.snowflake import SnowflakeDestination
        from destinations.postgresql import PostgreSQLDestination
        from decimal import Decimal
        from datetime import date, datetime
        import json

        try:
            pipeline_id = job["pipeline_id"]
            table_name = job["table_name"]

            logger.debug(
                f"Processing {len(records)} records to destinations for pipeline {pipeline_id}"
            )

            # Get pipeline with destinations
            pipeline = PipelineRepository.get_by_id(pipeline_id, include_relations=True)

            if not pipeline or not pipeline.destinations:
                logger.warning(f"Pipeline {pipeline_id} has no destinations configured")
                return

            # Convert records to CDC format with proper serialization
            cdc_records = []
            for record in records:
                # Serialize problematic types (Decimal, datetime, etc.)
                serialized_record = self._serialize_record(record)

                cdc_record = CDCRecord(
                    operation="r",  # 'r' = read/snapshot operation
                    table_name=table_name,
                    key=self._extract_keys(serialized_record),
                    value=serialized_record,
                    schema=None,
                    timestamp=None,
                )
                cdc_records.append(cdc_record)

            # Write batch to each destination
            for pd in pipeline.destinations:
                # Find matching table sync
                table_sync = next(
                    (ts for ts in pd.table_syncs if ts.table_name == table_name),
                    None,
                )

                if not table_sync:
                    logger.debug(
                        f"No table sync for {table_name} in destination {pd.destination_id}"
                    )
                    continue

                try:
                    # Get destination config
                    destination_config = DestinationRepository.get_by_id(
                        pd.destination_id
                    )
                    if not destination_config:
                        logger.warning(f"Destination {pd.destination_id} not found")
                        continue

                    # Create destination instance
                    if (
                        destination_config.type.upper()
                        == DestinationType.SNOWFLAKE.value
                    ):
                        destination = SnowflakeDestination(destination_config)
                    elif (
                        destination_config.type.upper()
                        == DestinationType.POSTGRES.value
                    ):
                        destination = PostgreSQLDestination(destination_config)
                    else:
                        logger.warning(
                            f"Unsupported destination type: {destination_config.type}"
                        )
                        continue

                    # Initialize destination
                    destination.initialize()

                    # Write batch to destination
                    written = destination.write_batch(cdc_records, table_sync)
                    logger.info(
                        f"✓ Wrote {written} records to destination {destination_config.name} "
                        f"for table {table_name} -> {table_sync.table_name_target}"
                    )

                    # Track data flow monitoring (same as CDC)
                    if written > 0:
                        try:
                            DataFlowRepository.increment_count(
                                pipeline_id=pipeline_id,
                                pipeline_destination_id=pd.id,
                                source_id=job["source_id"],
                                table_sync_id=table_sync.id,
                                table_name=f"LANDING_{table_name.upper()}",
                                count=written,
                            )
                            logger.debug(
                                f"Tracked {written} records in data_flow_record_monitoring "
                                f"for pipeline {pipeline_id}, destination {pd.id}"
                            )
                        except Exception as monitoring_error:
                            logger.warning(
                                f"Failed to update data flow monitoring: {monitoring_error}"
                            )

                except Exception as dest_error:
                    logger.error(
                        f"Failed to write batch to destination {pd.destination_id}: {dest_error}",
                        exc_info=True,
                    )
                    # Continue with other destinations even if one fails

        except Exception as e:
            logger.error(f"Error processing batch to destinations: {e}", exc_info=True)
            raise

    def _extract_keys(self, record: dict) -> dict:
        """
        Extract primary key fields from record.

        Args:
            record: Record dictionary

        Returns:
            Dictionary with key fields (simple heuristic - look for 'id' field)
        """
        # Simple heuristic - in production you should query the table schema
        # to get actual primary keys
        if "id" in record:
            return {"id": record["id"]}
        return {}

    def _serialize_record(self, record: dict) -> dict:
        """
        Serialize record values to handle problematic types.

        Converts Decimal, datetime, date, and other non-JSON-serializable types
        to formats that Snowflake destinations can handle properly.

        DuckDB PostgreSQL Scanner Type Mapping:
        - PostgreSQL TIMESTAMP → Python datetime (naive, no tzinfo)
        - PostgreSQL TIMESTAMPTZ → Python datetime (aware, tzinfo preserved)
        - PostgreSQL NUMERIC/DECIMAL → Python Decimal
        - PostgreSQL DATE → Python date
        - PostgreSQL TIME → Python time
        - PostgreSQL UUID → Python UUID
        - PostgreSQL BYTEA → Python bytes

        Snowflake Target Type Mapping:
        - TIMESTAMP_NTZ ← datetime without timezone suffix
        - TIMESTAMP_TZ ← datetime with timezone suffix (preserves original TZ)
        - NUMBER/NUMERIC ← string (preserves precision)

        Args:
            record: Raw record dictionary from DuckDB

        Returns:
            Serialized record dictionary
        """
        from decimal import Decimal
        from datetime import date, datetime, time
        from uuid import UUID

        serialized = {}
        for key, value in record.items():
            if value is None:
                serialized[key] = None
            elif isinstance(value, Decimal):
                # Convert Decimal to string to preserve precision for Snowflake NUMERIC
                # DO NOT use float() as it loses precision for high-precision decimals
                serialized[key] = str(value)
            elif isinstance(value, datetime):
                # DuckDB returns:
                # - TIMESTAMP (without TZ) → naive datetime (no tzinfo)
                # - TIMESTAMPTZ (with TZ) → aware datetime (tzinfo preserved)
                #
                # Snowflake expects:
                # - TIMESTAMP_NTZ ← "2024-01-15T10:30:00.000000" (no TZ suffix)
                # - TIMESTAMP_TZ ← "2024-01-15T10:30:00.000000+07:00" (with TZ suffix, PRESERVED)
                if value.tzinfo is not None:
                    # Has timezone info → TIMESTAMPTZ → TIMESTAMP_TZ
                    # IMPORTANT: Preserve the original timezone, DO NOT convert to UTC
                    # isoformat() keeps the original timezone offset
                    serialized[key] = value.isoformat()
                else:
                    # No timezone info → TIMESTAMP → TIMESTAMP_NTZ
                    # Output without timezone for Snowflake TIMESTAMP_NTZ
                    serialized[key] = value.strftime("%Y-%m-%dT%H:%M:%S.%f")
            elif isinstance(value, date):
                # DATE → ISO format string "YYYY-MM-DD"
                serialized[key] = value.isoformat()
            elif isinstance(value, time):
                # TIME → ISO format string with or without TZ
                if value.tzinfo is not None:
                    # TIME WITH TIME ZONE → preserve timezone
                    serialized[key] = value.isoformat()
                else:
                    # TIME WITHOUT TIME ZONE → no offset
                    serialized[key] = value.strftime("%H:%M:%S.%f")
            elif isinstance(value, UUID):
                # UUID → string
                serialized[key] = str(value)
            elif isinstance(value, (bytes, bytearray)):
                # BYTEA/geometry WKB → hex string
                serialized[key] = value.hex()
            elif isinstance(value, dict):
                # JSON/JSONB → keep as dict for VARIANT
                serialized[key] = value
            elif isinstance(value, list):
                # Array types → keep as list
                serialized[key] = value
            elif isinstance(value, bool):
                # Boolean → keep as-is
                serialized[key] = value
            elif isinstance(value, (int, float)):
                # Numeric primitives → keep as-is
                serialized[key] = value
            elif isinstance(value, str):
                # String → keep as-is
                serialized[key] = value
            else:
                # Unknown types → convert to string
                logger.warning(
                    f"Unknown type {type(value).__name__} for column {key}, converting to string"
                )
                serialized[key] = str(value)

        return serialized

    def _update_job_status(
        self,
        job_id: int,
        status: str,
        count_record: Optional[int] = None,
        error_message: Optional[str] = None,
    ) -> None:
        """
        Update backfill job status in database.

        Args:
            job_id: Job ID
            status: New status
            count_record: Optional record count
            error_message: Optional error message for failed jobs
        """
        pool = get_connection_pool()
        conn = None

        # Set is_error flag if status is FAILED
        is_error = status == BackfillStatus.FAILED.value

        try:
            conn = pool.getconn()
            with conn.cursor() as cursor:
                if count_record is not None and error_message is not None:
                    cursor.execute(
                        """
                        UPDATE queue_backfill_data
                        SET status = %s, count_record = %s, error_message = %s, is_error = %s, updated_at = NOW()
                        WHERE id = %s
                        """,
                        (status, count_record, error_message, is_error, job_id),
                    )
                elif count_record is not None:
                    cursor.execute(
                        """
                        UPDATE queue_backfill_data
                        SET status = %s, count_record = %s, is_error = %s, updated_at = NOW()
                        WHERE id = %s
                        """,
                        (status, count_record, is_error, job_id),
                    )
                elif error_message is not None:
                    cursor.execute(
                        """
                        UPDATE queue_backfill_data
                        SET status = %s, error_message = %s, is_error = %s, updated_at = NOW()
                        WHERE id = %s
                        """,
                        (status, error_message, is_error, job_id),
                    )
                else:
                    cursor.execute(
                        """
                        UPDATE queue_backfill_data
                        SET status = %s, is_error = %s, updated_at = NOW()
                        WHERE id = %s
                        """,
                        (status, is_error, job_id),
                    )
                conn.commit()
        except Exception as e:
            logger.error(f"Error updating job status: {e}")
            if conn:
                conn.rollback()
        finally:
            if conn:
                pool.putconn(conn)

    def _update_job_count(self, job_id: int, count: int) -> None:
        """
        Update job record count.

        Args:
            job_id: Job ID
            count: Current count
        """
        pool = get_connection_pool()
        conn = None

        try:
            conn = pool.getconn()
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE queue_backfill_data
                    SET count_record = %s
                    WHERE id = %s
                    """,
                    (count, job_id),
                )
                conn.commit()
        except Exception as e:
            logger.error(f"Error updating job count: {e}")
            if conn:
                conn.rollback()
        finally:
            if conn:
                pool.putconn(conn)

    def _update_job_total_record(self, job_id: int, total: int) -> None:
        """
        Update job total record count.

        Args:
            job_id: Job ID
            total: Total number of records to process
        """
        pool = get_connection_pool()
        conn = None

        try:
            conn = pool.getconn()
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE queue_backfill_data
                    SET total_record = %s, updated_at = NOW()
                    WHERE id = %s
                    """,
                    (total, job_id),
                )
                conn.commit()
        except Exception as e:
            logger.error(f"Error updating job total record: {e}")
            if conn:
                conn.rollback()
        finally:
            if conn:
                pool.putconn(conn)

    def _is_job_cancelled(self, job_id: int) -> bool:
        """
        Check if job was cancelled.

        Args:
            job_id: Job ID

        Returns:
            True if cancelled
        """
        pool = get_connection_pool()
        conn = None

        try:
            conn = pool.getconn()
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT status FROM queue_backfill_data WHERE id = %s",
                    (job_id,),
                )
                result = cursor.fetchone()
                return result and result[0] == BackfillStatus.CANCELLED.value
        except Exception as e:
            logger.error(f"Error checking job cancellation: {e}")
            return False
        finally:
            if conn:
                pool.putconn(conn)
