"""
WAL Monitor service for background monitoring.

Implements PostgreSQL WAL status monitoring with retry logic and error handling.
"""

import asyncio
from datetime import datetime
from typing import Optional

import psycopg2
import psycopg2.extras
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.core.database import get_session_context
from app.core.exceptions import WALMonitorError
from app.core.logging import get_logger
from app.domain.models.source import Source
from app.domain.repositories.source import SourceRepository
from app.domain.repositories.wal_monitor_repo import WALMonitorRepository

logger = get_logger(__name__)


class WALMonitorService:
    """
    Service for monitoring PostgreSQL WAL status.

    Periodically checks WAL status for all registered sources and
    persists current status to the wal_monitor table (upsert pattern).
    """

    # SQL query to get current WAL LSN, position, replication lag, and total WAL size
    WAL_STATUS_QUERY = """
        SELECT 
            pg_current_wal_lsn()::text AS wal_lsn,
            pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')::bigint AS wal_position,
            (
                SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint
                FROM pg_replication_slots
                WHERE slot_name = %(slot_name)s
            ) AS replication_lag,
            (
                SELECT pg_size_pretty(sum(size)) 
                FROM pg_ls_waldir()
            ) AS total_wal_size;
    """

    def __init__(self):
        """Initialize WAL monitor service."""
        self.settings = get_settings()
        self._running = False
        self._task: Optional[asyncio.Task] = None

    async def check_wal_status(self, source: Source) -> dict:
        """
        Check WAL status for a specific source.

        Connects to the source database and queries current WAL status.

        Args:
            source: Source to check

        Returns:
            Dictionary with wal_lsn, wal_position, replication_lag, and total_wal_size

        Raises:
            WALMonitorError: If WAL check fails
        """

        def _check_wal_sync():
            """Synchronous WAL check using psycopg2."""
            connection = None
            try:
                logger.debug(
                    "Checking WAL status",
                    extra={
                        "source_id": source.id,
                        "host": source.pg_host,
                        "database": source.pg_database,
                        "slot_name": source.publication_name,
                    },
                )

                # Create connection to source database
                connection = psycopg2.connect(
                    host=source.pg_host,
                    port=source.pg_port,
                    database=source.pg_database,
                    user=source.pg_username,
                    password=source.pg_password,
                    connect_timeout=self.settings.wal_monitor_timeout_seconds,
                )

                # Execute WAL status query
                with connection.cursor(
                    cursor_factory=psycopg2.extras.RealDictCursor
                ) as cursor:
                    # Pass the publication name as the slot name
                    cursor.execute(
                        self.WAL_STATUS_QUERY, {"slot_name": source.publication_name}
                    )
                    result = cursor.fetchone()
                    wal_lsn = result["wal_lsn"]
                    wal_position = result["wal_position"]
                    replication_lag = result["replication_lag"]
                    total_wal_size = result["total_wal_size"]

                logger.info(
                    "WAL status checked successfully",
                    extra={
                        "source_id": source.id,
                        "wal_lsn": wal_lsn,
                        "wal_position": wal_position,
                        "replication_lag": replication_lag,
                        "total_wal_size": total_wal_size,
                        "wal_position_mb": wal_position / (1024 * 1024),
                    },
                )

                return {
                    "wal_lsn": wal_lsn,
                    "wal_position": wal_position,
                    "replication_lag": replication_lag,
                    "total_wal_size": total_wal_size,
                }

            except psycopg2.Error as e:
                logger.error(
                    "PostgreSQL error while checking WAL size",
                    extra={"source_id": source.id, "error": str(e)},
                )
                raise WALMonitorError(
                    source_id=source.id, message=f"PostgreSQL error: {str(e)}"
                ) from e
            except Exception as e:
                logger.error(
                    "Unexpected error while checking WAL size",
                    extra={"source_id": source.id, "error": str(e)},
                )
                raise WALMonitorError(
                    source_id=source.id, message=f"Unexpected error: {str(e)}"
                ) from e
            finally:
                if connection:
                    connection.close()

        # Run synchronous function in thread pool to not block event loop
        return await asyncio.to_thread(_check_wal_sync)

    async def monitor_source(self, source: Source, db: Session) -> None:
        """
        Monitor WAL status for a single source with retry logic.

        Args:
            source: Source to monitor
            db: Database session for persisting status
        """
        max_retries = self.settings.wal_monitor_max_retries
        retry_count = 0

        while retry_count <= max_retries:
            try:
                # Check WAL status
                wal_status = await self.check_wal_status(source)
                now = datetime.utcnow()

                # Upsert monitor record
                wal_monitor_repo = WALMonitorRepository(db)
                wal_monitor_repo.upsert_monitor(
                    source_id=source.id,
                    wal_lsn=wal_status["wal_lsn"],
                    wal_position=wal_status["wal_position"],
                    last_wal_received=now,
                    last_transaction_time=now,
                    replication_slot_name=source.publication_name,  # Using publication name as identifier
                    replication_lag_bytes=wal_status["replication_lag"],
                    total_wal_size=wal_status["total_wal_size"],
                    status="ACTIVE",
                    error_message=None,
                )
                db.commit()

                logger.info(
                    "WAL monitor status updated",
                    extra={
                        "source_id": source.id,
                        "wal_lsn": wal_status["wal_lsn"],
                        "lag_bytes": wal_status["replication_lag"],
                        "total_size": wal_status["total_wal_size"],
                        "wal_position_mb": wal_status["wal_position"] / (1024 * 1024),
                    },
                )

                return  # Success, exit retry loop

            except WALMonitorError as e:
                retry_count += 1
                if retry_count <= max_retries:
                    logger.warning(
                        f"WAL monitor retry {retry_count}/{max_retries}",
                        extra={"source_id": source.id, "error": str(e)},
                    )
                    await asyncio.sleep(2**retry_count)  # Exponential backoff
                else:
                    logger.error(
                        "WAL monitor failed after all retries",
                        extra={"source_id": source.id, "retries": retry_count},
                    )
                    # Update status to ERROR
                    try:
                        wal_monitor_repo = WALMonitorRepository(db)
                        wal_monitor_repo.upsert_monitor(
                            source_id=source.id,
                            status="ERROR",
                            error_message=str(e),
                            last_wal_received=datetime.utcnow(),
                        )
                        db.commit()
                    except Exception as update_error:
                        logger.error(
                            "Failed to update error status",
                            extra={"source_id": source.id, "error": str(update_error)},
                        )
            except Exception as e:
                logger.error(
                    "Unexpected error in WAL monitoring",
                    extra={"source_id": source.id, "error": str(e)},
                )
                # Update status to ERROR
                try:
                    wal_monitor_repo = WALMonitorRepository(db)
                    wal_monitor_repo.upsert_monitor(
                        source_id=source.id,
                        status="ERROR",
                        error_message=str(e),
                        last_wal_received=datetime.utcnow(),
                    )
                    db.commit()
                except Exception as update_error:
                    logger.error(
                        "Failed to update error status",
                        extra={"source_id": source.id, "error": str(update_error)},
                    )
                break

    async def monitor_all_sources(self) -> None:
        """
        Monitor WAL status for all active sources.

        This method is called periodically by the background scheduler.
        """
        try:
            logger.info("Starting WAL monitoring cycle")

            # Use regular 'with' instead of 'async with' for synchronous context manager
            with get_session_context() as db:
                # Get all sources
                source_repo = SourceRepository(db)
                sources = source_repo.get_all(skip=0, limit=1000)

                if not sources:
                    logger.info("No sources to monitor")
                    return

                logger.info(f"Monitoring WAL for {len(sources)} sources")

                # Monitor each source concurrently
                tasks = [self.monitor_source(source, db) for source in sources]
                await asyncio.gather(*tasks, return_exceptions=True)

                logger.info("WAL monitoring cycle completed")

        except Exception as e:
            logger.error("Error in WAL monitoring cycle", extra={"error": str(e)})

    async def start(self) -> None:
        """
        Start the WAL monitoring background task.

        Runs monitoring at configured intervals.
        """
        if self._running:
            logger.warning("WAL monitor is already running")
            return

        if not self.settings.wal_monitor_enabled:
            logger.info("WAL monitoring is disabled in configuration")
            return

        self._running = True
        logger.info(
            "Starting WAL monitor",
            extra={"interval_seconds": self.settings.wal_monitor_interval_seconds},
        )

        # Start monitoring loop
        self._task = asyncio.create_task(self._monitoring_loop())

    async def stop(self) -> None:
        """Stop the WAL monitoring background task."""
        if not self._running:
            return

        logger.info("Stopping WAL monitor")
        self._running = False

        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

        logger.info("WAL monitor stopped")

    async def _monitoring_loop(self) -> None:
        """
        Main monitoring loop.

        Runs monitoring at configured intervals until stopped.
        """
        while self._running:
            try:
                await self.monitor_all_sources()
                await asyncio.sleep(self.settings.wal_monitor_interval_seconds)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Error in monitoring loop", extra={"error": str(e)})
                # Continue monitoring despite errors
                await asyncio.sleep(60)  # Wait a minute before retry
