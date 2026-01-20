"""
Pipeline service containing business logic.

Implements business rules and orchestrates repository operations for pipelines.
"""

from typing import List

from sqlalchemy.orm import Session

from app.core.exceptions import DuplicateEntityError
from app.core.logging import get_logger
from app.domain.models.pipeline import Pipeline, PipelineMetadata, PipelineStatus
from app.domain.repositories.pipeline import PipelineRepository
from app.domain.schemas.pipeline import PipelineCreate, PipelineUpdate
from app.domain.services.source import SourceService
from app.domain.models.data_flow_monitoring import DataFlowRecordMonitoring
from sqlalchemy import func, desc, and_
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

logger = get_logger(__name__)


class PipelineService:
    """
    Service layer for Pipeline entity.

    Implements business logic for managing ETL pipeline configurations.
    """

    def __init__(self, db: Session):
        """Initialize pipeline service."""
        self.db = db
        self.repository = PipelineRepository(db)

    def create_pipeline(self, pipeline_data: PipelineCreate) -> Pipeline:
        """
        Create a new pipeline with associated metadata.

        Args:
            pipeline_data: Pipeline creation data

        Returns:
            Created pipeline
        """
        logger.info("Creating new pipeline", extra={"name": pipeline_data.name})

        # Check if source is already used in another pipeline
        existing_pipelines = self.repository.get_by_source_id(pipeline_data.source_id)
        if existing_pipelines:
            raise DuplicateEntityError(
                entity_type="Pipeline",
                field="source_id",
                value=pipeline_data.source_id,
                details={"message": "Source is already connected to a pipeline"},
            )

        # Force status to PAUSE for initialization
        pipeline_data.status = PipelineStatus.PAUSE
        
        # Create pipeline with metadata using repository method
        pipeline = self.repository.create_with_metadata(**pipeline_data.dict())

        logger.info(
            "Pipeline created successfully",
            extra={"pipeline_id": pipeline.id, "name": pipeline.name},
        )

        return pipeline

    def get_pipeline(self, pipeline_id: int) -> Pipeline:
        """
        Get pipeline by ID with all related entities.

        Args:
            pipeline_id: Pipeline identifier

        Returns:
            Pipeline entity with relations
        """
        return self.repository.get_by_id_with_relations(pipeline_id)

    def get_pipeline_by_name(self, name: str) -> Pipeline | None:
        """
        Get pipeline by name.

        Args:
            name: Pipeline name

        Returns:
            Pipeline entity or None
        """
        return self.repository.get_by_name(name)

    def list_pipelines(self, skip: int = 0, limit: int = 100) -> List[Pipeline]:
        """
        List all pipelines with pagination.

        Args:
            skip: Number of pipelines to skip
            limit: Maximum number of pipelines to return

        Returns:
            List of pipelines with relations
        """
        return self.repository.get_all_with_relations(skip=skip, limit=limit)

    def list_pipelines_by_status(
        self, status: PipelineStatus, skip: int = 0, limit: int = 100
    ) -> List[Pipeline]:
        """
        List pipelines filtered by status.

        Args:
            status: Pipeline status to filter by
            skip: Number of pipelines to skip
            limit: Maximum number of pipelines to return

        Returns:
            List of pipelines with specified status
        """
        return self.repository.get_by_status(status=status, skip=skip, limit=limit)

    def count_pipelines(self) -> int:
        """
        Count total number of pipelines.

        Returns:
            Total count
        """
        return self.repository.count()

    def update_pipeline(
        self, pipeline_id: int, pipeline_data: PipelineUpdate
    ) -> Pipeline:
        """
        Update an existing pipeline.

        Args:
            pipeline_id: Pipeline identifier
            pipeline_data: Pipeline update data

        Returns:
            Updated pipeline
        """
        logger.info(
            "Updating pipeline",
            extra={
                "pipeline_id": pipeline_id,
                "fields": pipeline_data.dict(exclude_unset=True),
            },
        )

        # Get existing pipeline to ensure it exists
        pipeline = self.repository.get_by_id(pipeline_id)

        # Update pipeline
        updated_pipeline = self.repository.update(
            pipeline_id, **pipeline_data.dict(exclude_unset=True)
        )

        logger.info(
            "Pipeline updated successfully",
            extra={"pipeline_id": updated_pipeline.id, "name": updated_pipeline.name},
        )

        return updated_pipeline

    def delete_pipeline(self, pipeline_id: int) -> None:
        """
        Delete a pipeline and its associated metadata.

        Args:
            pipeline_id: Pipeline identifier
        """
        logger.info("Deleting pipeline", extra={"pipeline_id": pipeline_id})

        # Verify pipeline exists before deletion
        self.repository.get_by_id(pipeline_id)

        # Delete pipeline (metadata will cascade)
        self.repository.delete(pipeline_id)

        logger.info("Pipeline deleted successfully", extra={"pipeline_id": pipeline_id})

    def start_pipeline(self, pipeline_id: int) -> Pipeline:
        """
        Start a pipeline by setting its status to START.

        Args:
            pipeline_id: Pipeline identifier

        Returns:
            Updated pipeline
        """
        logger.info("Starting pipeline", extra={"pipeline_id": pipeline_id})

        pipeline = self.repository.get_by_id(pipeline_id)
        pipeline.start()

        # Update metadata status to RUNNING
        if pipeline.pipeline_metadata:
            pipeline.pipeline_metadata.set_running()

        self.db.commit()
        self.db.refresh(pipeline)

        logger.info("Pipeline started successfully", extra={"pipeline_id": pipeline_id})

        return pipeline

    def pause_pipeline(self, pipeline_id: int) -> Pipeline:
        """
        Pause a pipeline by setting its status to PAUSE.

        Args:
            pipeline_id: Pipeline identifier

        Returns:
            Updated pipeline
        """
        logger.info("Pausing pipeline", extra={"pipeline_id": pipeline_id})

        pipeline = self.repository.get_by_id(pipeline_id)
        pipeline.pause()

        # Update metadata status to PAUSED
        if pipeline.pipeline_metadata:
            pipeline.pipeline_metadata.set_paused()

        self.db.commit()
        self.db.refresh(pipeline)

        logger.info("Pipeline paused successfully", extra={"pipeline_id": pipeline_id})

        return pipeline

    def refresh_pipeline(self, pipeline_id: int) -> Pipeline:
        """
        Trigger a pipeline refresh by setting its status to REFRESH.

        Args:
            pipeline_id: Pipeline identifier

        Returns:
            Updated pipeline
        """
        logger.info("Refreshing pipeline", extra={"pipeline_id": pipeline_id})

        pipeline = self.repository.get_by_id(pipeline_id)
        pipeline.refresh()

        self.db.commit()
        self.db.refresh(pipeline)

        logger.info("Pipeline refresh triggered", extra={"pipeline_id": pipeline_id})

        return pipeline

    def record_pipeline_error(self, pipeline_id: int, error_message: str) -> Pipeline:
        """
        Record an error for a pipeline.

        Args:
            pipeline_id: Pipeline identifier
            error_message: Error description

        Returns:
            Updated pipeline
        """
        logger.error(
            "Recording pipeline error",
            extra={"pipeline_id": pipeline_id, "error": error_message},
        )

        pipeline = self.repository.get_by_id(pipeline_id)

        if pipeline.pipeline_metadata:
            pipeline.pipeline_metadata.set_error(error_message)

        self.db.commit()
        self.db.refresh(pipeline)

        return pipeline

    def clear_pipeline_error(self, pipeline_id: int) -> Pipeline:
        """
        Clear error state for a pipeline.

        Args:
            pipeline_id: Pipeline identifier

        Returns:
            Updated pipeline
        """
        logger.info("Clearing pipeline error", extra={"pipeline_id": pipeline_id})

        pipeline = self.repository.get_by_id(pipeline_id)

        if pipeline.pipeline_metadata:
            pipeline.pipeline_metadata.clear_error()

        self.db.commit()
        self.db.refresh(pipeline)

        logger.info("Pipeline error cleared", extra={"pipeline_id": pipeline_id})

        return pipeline

    def initialize_pipeline(self, pipeline_id: int) -> None:
        """
        Background task to initialize pipeline resources in Snowflake.
        """
        logger.info("Starting pipeline initialization", extra={"pipeline_id": pipeline_id})
        
        try:
            # 1. Get Pipeline and Progress
            pipeline = self.repository.get_by_id_with_relations(pipeline_id)
            progress = pipeline.pipeline_progress
            
            self._update_progress(progress, 0, "Starting initialization", "IN_PROGRESS")
            
            # 2. Get Source Tables
            self._update_progress(progress, 10, "Fetching source tables", "IN_PROGRESS")
            source_service = SourceService(self.db)
            source_details = source_service.get_source_details(pipeline.source_id)
            
            # Use source_details.tables (list of source_detail.SourceTableInfo)
            # SourceTableInfo has: id, table_name, schema_table (List[dict])
            tables = source_details.tables
            
            if not tables:
                self._update_progress(progress, 100, "No tables to process", "COMPLETED")
                pipeline.status = PipelineStatus.START.value
                self.db.commit()
                return

            # 3. Connect to Snowflake
            self._update_progress(progress, 20, "Connecting to Snowflake", "IN_PROGRESS")
            conn = self._get_snowflake_connection(pipeline.destination)
            cursor = conn.cursor()
            
            try:
                # Set context
                landing_db = pipeline.destination.snowflake_landing_database
                landing_schema = pipeline.destination.snowflake_landing_schema
                target_db = pipeline.destination.snowflake_database
                target_schema = pipeline.destination.snowflake_schema

                # Validate configuration
                if not all([landing_db, landing_schema, target_db, target_schema]):
                    raise ValueError(
                        f"Destination configuration incomplete for pipeline {pipeline.name}. "
                        "Ensure landing_database, landing_schema, database, and schema are set."
                    )
                
                # Check Databases/Schemas existence? usually assumed or created.
                # Just use them.
                
                # Need TableMetadataRepository to update flags
                from app.domain.repositories.table_metadata_repo import TableMetadataRepository
                tm_repo = TableMetadataRepository(self.db)
                
                total_tables = len(tables)
                for index, table in enumerate(tables):
                    current_percent = 20 + int((index / total_tables) * 70) 
                    self._update_progress(progress, current_percent, f"Processing table: {table.table_name}", "IN_PROGRESS")
                    
                    table_name = table.table_name
                    columns = table.schema_definition # List of column dicts
                    table_id = table.id # This is table_metadata_list.id from SourceTableInfo
                    
                    # 4. Generate & Execute DDLs
                    
                    # A. Landing Table
                    landing_table = f"LANDING_{table_name}"
                    landing_ddl = self._generate_landing_ddl(landing_db, landing_schema, landing_table, columns)
                    cursor.execute(landing_ddl)
                    tm_repo.update_status(table_id, is_exists_table_landing=True)
                    
                    # B. Stream
                    stream_name = f"STREAM_{landing_table}"
                    stream_ddl = f"CREATE OR REPLACE STREAM {landing_db}.{landing_schema}.{stream_name} ON TABLE {landing_db}.{landing_schema}.{landing_table}"
                    cursor.execute(stream_ddl)
                    tm_repo.update_status(table_id, is_exists_stream=True)
                    
                    # C. Destination Table
                    target_table = table_name
                    target_ddl = self._generate_target_ddl(target_db, target_schema, target_table, columns)
                    cursor.execute(target_ddl)
                    tm_repo.update_status(table_id, is_exists_table_destination=True)
                    
                    # D. Merge Task
                    task_name = f"TASK_MERGE_{table_name}"
                    task_ddl = self._generate_merge_task_ddl(
                        pipeline,
                        landing_db, landing_schema, landing_table,
                        stream_name,
                        target_db, target_schema, target_table,
                        columns
                    )
                    cursor.execute(task_ddl)
                    cursor.execute(f"ALTER TASK {landing_db}.{landing_schema}.{task_name} RESUME")
                    tm_repo.update_status(table_id, is_exists_task=True)

                
                # 5. Finalize
                self._update_progress(progress, 100, "Initialization completed", "COMPLETED")
                pipeline.status = PipelineStatus.START.value
                self.db.commit()
                
            finally:
                cursor.close()
                conn.close()
                
        except Exception as e:
            logger.error(f"Pipeline initialization failed: {e}", exc_info=True)
            # Re-fetch progress attached to session if needed, but it should be attached
            try:
                 if progress:
                    self._update_progress(progress, progress.progress, "Initialization failed", "FAILED", str(e))
            except:
                 pass

    def _update_progress(self, progress, percent, step, status, details=None):
        progress.progress = percent
        progress.step = step
        progress.status = status
        if details:
            progress.details = details
        self.db.commit()

    def _get_snowflake_connection(self, destination):
        private_key_str = destination.snowflake_private_key.strip()
        passphrase = None
        if destination.snowflake_private_key_passphrase:
            passphrase = destination.snowflake_private_key_passphrase.encode()

        p_key = serialization.load_pem_private_key(
            private_key_str.encode(),
            password=passphrase,
            backend=default_backend(),
        )
        pkb = p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )

        return snowflake.connector.connect(
            user=destination.snowflake_user,
            account=destination.snowflake_account,
            private_key=pkb,
            role=destination.snowflake_role,
            warehouse=destination.snowflake_warehouse,
            database=destination.snowflake_database,
            schema=destination.snowflake_schema,
            client_session_keep_alive=False,
            application="Rosetta_ETL"
        )

    def _map_postgres_to_snowflake(self, col: dict, for_landing: bool = False) -> str:
        """
        Map PostgreSQL data type to Snowflake data type.
        
        Args:
            col: Column metadata dict containing real_data_type, numeric_precision, etc.
            for_landing: If True, spatial types (GEOGRAPHY/GEOMETRY) will be mapped to VARCHAR
                         since data arrives as text from PostgreSQL WAL.
        
        Returns:
            Snowflake data type string
        """
        # col contains: column_name, real_data_type, numeric_precision, numeric_scale, ...
        # Note: input might be from SchemaMonitor ('real_data_type') or just 'data_type' if from old metadata
        
        pg_type = str(col.get('real_data_type') or col.get('data_type')).upper()
        precision = col.get('numeric_precision')
        scale = col.get('numeric_scale')

        if "INT" in pg_type or "SERIAL" in pg_type:
            return "NUMBER(38,0)"
        elif "NUMERIC" in pg_type or "DECIMAL" in pg_type:
            if precision is not None and scale is not None:
                return f"NUMBER({precision}, {scale})"
            return "NUMBER(38,4)" 
        elif "FLOAT" in pg_type or "DOUBLE" in pg_type:
            return "FLOAT"
        elif "REAL" in pg_type:
            return "FLOAT"
        elif "BOOL" in pg_type:
            return "BOOLEAN"
        elif "DATE" in pg_type:
            return "DATE"
        elif "TIMESTAMP" in pg_type:
            return "TIMESTAMP_TZ"
        elif "JSON" in pg_type:
            return "VARIANT"
        elif "ARRAY" in pg_type:
            return "ARRAY"
        elif "UUID" in pg_type:
            return "VARCHAR(36)"
        elif "GEOGRAPHY" in pg_type:
            # Landing table receives text from WAL, target table uses native type
            return "VARCHAR" if for_landing else "GEOGRAPHY"
        elif "GEOMETRY" in pg_type:
            # Landing table receives text from WAL, target table uses native type
            return "VARCHAR" if for_landing else "GEOMETRY"
        else:
            return "VARCHAR"

    def _generate_landing_ddl(self, db, schema, table_name, columns):
        cols_ddl = []
        for col in columns:
            col_name = col['column_name']
            # Pass entire col dict to mapper, with for_landing=True to use VARCHAR for spatial types
            sf_type = self._map_postgres_to_snowflake(col, for_landing=True)
            cols_ddl.append(f"{col_name} {sf_type}")
            
        cols_ddl.append("operation VARCHAR(1)")
        cols_ddl.append("sync_timestamp_rosetta TIMESTAMP_TZ")
        
        ddl = f"CREATE TABLE IF NOT EXISTS {db}.{schema}.{table_name} ({', '.join(cols_ddl)}) ENABLE_SCHEMA_EVOLUTION = TRUE"
        return ddl

    def _generate_target_ddl(self, db, schema, table_name, columns):
        # Precise type (mapped), no default value, primary key
        
        cols_ddl = []
        pks = []
        
        for col in columns:
            col_name = col['column_name']
            sf_type = self._map_postgres_to_snowflake(col)
            
            # Basic column definition
            definition = f"{col_name} {sf_type}"
            cols_ddl.append(definition)
            
            # Check PK
            if col.get('is_primary_key') is True:
                 pks.append(col_name)
                 
        # Add PK constraint if exists
        if pks:
            pk_cols = ", ".join(pks)
            # Snowflake supports inline or out-of-line. Out-of-line is cleaner for composites or naming.
            cols_ddl.append(f"CONSTRAINT pk_{table_name} PRIMARY KEY ({pk_cols})")
            
        cols_definition = ",\n            ".join(cols_ddl)
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {db}.{schema}.{table_name} (
            {cols_definition}
        ) ENABLE_SCHEMA_EVOLUTION = TRUE;
        """
        return ddl

    def _generate_merge_task_ddl(self, pipeline, l_db, l_schema, l_table, stream, t_db, t_schema, t_table, columns):
        # 1. Try to find explicit PK
        pk_cols = []
        for col in columns:
             if col.get('is_primary_key') is True:
                 pk_cols.append(col['column_name'])
        
        # 2. Fallback to 'id' or first column if no PK found
        if not pk_cols:
            for col in columns:
                if 'id' in col['column_name'].lower():
                    pk_cols.append(col['column_name'])
                    break
        if not pk_cols:
            pk_cols.append(columns[0]['column_name'])
        
        # Prepare JOIN condition for MERGE
        # T.id = S.id AND T.key2 = S.key2
        join_condition = " AND ".join([f"T.{pk} = S.{pk}" for pk in pk_cols])
        
        # Prepare Partition By columns for De-duplication
        partition_by = ", ".join(pk_cols)
        
        col_names = [c['column_name'] for c in columns]
        
        # Indent utility
        indent = "            "
        
        # Build a mapping of column names to their PostgreSQL types for spatial conversion
        col_type_map = {}
        for col in columns:
            pg_type = str(col.get('real_data_type') or col.get('data_type')).upper()
            col_type_map[col['column_name']] = pg_type
        
        def get_source_value(col_name: str) -> str:
            """Get the source value expression, applying spatial conversion if needed."""
            pg_type = col_type_map.get(col_name, '')
            if 'GEOGRAPHY' in pg_type:
                return f"TRY_TO_GEOGRAPHY(S.{col_name})"
            elif 'GEOMETRY' in pg_type:
                return f"TRY_TO_GEOMETRY(S.{col_name})"
            return f"S.{col_name}"
        
        # UPDATE SET clause
        # exclude PKs from update usually? MERGE allows updating everything except join keys usually.
        # But safest to update all non-PKs.
        update_cols = [c for c in col_names if c not in pk_cols]
        if not update_cols:
             # Edge case: table only has PK columns? Then update is no-op usually or not possible.
             # Or maybe just update one col to itself? Dummy update?
             # Let's assume there's always data columns. If not, we might not need update clause, just insert.
             # But for safety, let's keep all columns if no distinct non-PKs (shouldn't happen in real ETL).
             set_clause = ", ".join([f"{c} = {get_source_value(c)}" for c in col_names])
        else:
             set_clause = f",\n{indent}            ".join([f"{c} = {get_source_value(c)}" for c in update_cols])
        
        val_clause = ", ".join([get_source_value(c) for c in col_names])
        col_list = ", ".join(col_names)
        
        # Use Snowflake scripting block to run MERGE then DELETE from landing table
        task_ddl = f"""
        CREATE OR REPLACE TASK {l_db}.{l_schema}.TASK_MERGE_{t_table}
        WAREHOUSE = {pipeline.destination.snowflake_warehouse}
        SCHEDULE = '60 MINUTE'
        WHEN SYSTEM$STREAM_HAS_DATA('{l_db}.{l_schema}.{stream}')
        AS
        BEGIN
            -- Step 1: Merge data from stream to target table
            MERGE INTO {t_db}.{t_schema}.{t_table} AS T
            USING (
                SELECT * FROM (
                    SELECT 
                        *, 
                        ROW_NUMBER() OVER (PARTITION BY {partition_by} ORDER BY sync_timestamp_rosetta DESC) as rn
                    FROM {l_db}.{l_schema}.{stream}
                ) WHERE rn = 1
            ) AS S
            ON {join_condition}
            WHEN MATCHED AND S.operation = 'D' THEN
                DELETE
            WHEN MATCHED AND S.operation != 'D' THEN
                UPDATE SET 
                {set_clause}
            WHEN NOT MATCHED AND S.operation != 'D' THEN
                INSERT ({col_list})
                VALUES ({val_clause});
            
            -- Step 2: Clean up landing table after merge
            DELETE FROM {l_db}.{l_schema}.{l_table};
        END;
        """
        return task_ddl

    def get_pipeline_data_flow_stats(self, pipeline_id: int, days: int = 7) -> List[dict]:
        """
        Get data flow statistics for a pipeline, grouped by table and day.
        
        Args:
            pipeline_id: Pipeline identifier
            days: Number of days to look back
            
        Returns:
            List of stats per table
        """
        # 1. Get Source ID for pipeline
        pipeline = self.repository.get_by_id(pipeline_id)
        source_id = pipeline.source_id
        
        # 2. Daily Stats Query
        start_date = datetime.now(ZoneInfo('Asia/Jakarta')) - timedelta(days=days)
        
        daily_query = (
            self.db.query(
                DataFlowRecordMonitoring.table_name,
                func.date_trunc('day', DataFlowRecordMonitoring.created_at).label('day'),
                func.sum(DataFlowRecordMonitoring.record_count).label('total_count')
            )
            .filter(
                DataFlowRecordMonitoring.pipeline_id == pipeline_id,
                DataFlowRecordMonitoring.created_at >= start_date
            )
            .group_by(
                DataFlowRecordMonitoring.table_name,
                func.date_trunc('day', DataFlowRecordMonitoring.created_at)
            )
            .order_by(
                DataFlowRecordMonitoring.table_name,
                desc('day')
            )
        )
        
        daily_results = daily_query.all()
        
        # 3. Recent 5 Minutes Stats Query (for Monitoring chart)
        five_min_ago = datetime.now(ZoneInfo('Asia/Jakarta')) - timedelta(minutes=5)
        
        recent_query = (
            self.db.query(
                DataFlowRecordMonitoring.table_name,
                DataFlowRecordMonitoring.created_at,
                DataFlowRecordMonitoring.record_count
            )
            .filter(
                DataFlowRecordMonitoring.pipeline_id == pipeline_id,
                DataFlowRecordMonitoring.created_at >= five_min_ago
            )
            .order_by(DataFlowRecordMonitoring.created_at.asc())
        )
        
        recent_results = recent_query.all()
        
        # 4. Aggregating results by table
        stats_by_table = {}
        
        # Process Daily Stats
        for row in daily_results:
            table_name = row.table_name
            if table_name not in stats_by_table:
                stats_by_table[table_name] = {
                    "table_name": table_name,
                    "daily_stats": [],
                    "recent_stats": []
                }
            
            stats_by_table[table_name]["daily_stats"].append({
                "date": row.day.isoformat(),
                "count": row.total_count
            })

        # Process Recent Stats
        for row in recent_results:
            table_name = row.table_name
            if table_name not in stats_by_table:
                 stats_by_table[table_name] = {
                    "table_name": table_name,
                    "daily_stats": [],
                    "recent_stats": []
                }
            
            # Ensure timestamp is timezone-aware (Asia/Jakarta)
            timestamp = row.created_at
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=ZoneInfo('Asia/Jakarta'))
                
            stats_by_table[table_name]["recent_stats"].append({
                "timestamp": timestamp.isoformat(),
                "count": row.record_count
            })
            
        return list(stats_by_table.values())

