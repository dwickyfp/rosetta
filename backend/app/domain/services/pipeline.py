"""
Pipeline service containing business logic.

Implements business rules and orchestrates repository operations for pipelines.
"""

from typing import List

from sqlalchemy.orm import Session

from app.core.exceptions import DuplicateEntityError
from app.core.logging import get_logger
from app.domain.models.pipeline import Pipeline, PipelineMetadata, PipelineStatus, PipelineDestination, PipelineDestinationTableSync
from app.domain.repositories.pipeline import PipelineRepository
from app.domain.repositories.table_metadata_repo import TableMetadataRepository
from app.domain.schemas.pipeline import PipelineCreate, PipelineUpdate, PipelineDestinationResponse, PipelineDestinationTableSyncResponse, TableValidationResponse
from app.domain.services.source import SourceService
from app.domain.models.data_flow_monitoring import DataFlowRecordMonitoring
from app.core.security import decrypt_value
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

    def mark_ready_for_refresh(self, pipeline_id: int) -> None:
        """
        Mark pipeline as ready for refresh.
        
        Args:
            pipeline_id: Pipeline identifier
        """
        try:
            pipeline = self.repository.get_by_id(pipeline_id)
            pipeline.ready_refresh = True
            self.db.commit()
            logger.info(f"Marked pipeline {pipeline_id} as ready for refresh")
        except Exception as e:
            logger.error(f"Failed to mark pipeline {pipeline_id} for refresh: {e}")


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
        # Note: PipelineCreate no longer has destination_id
        pipeline = self.repository.create_with_metadata(**pipeline_data.dict())

        logger.info(
            "Pipeline created successfully",
            extra={"pipeline_id": pipeline.id, "name": pipeline.name},
        )

        return pipeline

    def add_pipeline_destination(self, pipeline_id: int, destination_id: int) -> Pipeline:
        """
        Add a destination to an existing pipeline.

        Args:
            pipeline_id: Pipeline identifier
            destination_id: Destination identifier

        Returns:
            Updated pipeline
        """
        logger.info(
            "Adding destination to pipeline",
            extra={"pipeline_id": pipeline_id, "destination_id": destination_id},
        )

        pipeline = self.repository.get_by_id(pipeline_id)
        
        # Check if destination already exists
        existing = (
            self.db.query(PipelineDestination)
            .filter_by(pipeline_id=pipeline_id, destination_id=destination_id)
            .first()
        )
        if existing:
            raise DuplicateEntityError(
                entity_type="PipelineDestination",
                field="destination_id",
                value=destination_id,
                details={"message": "Destination is already added to this pipeline"},
            )

        # Add destination
        new_dest = PipelineDestination(
            pipeline_id=pipeline_id, destination_id=destination_id
        )
        self.db.add(new_dest)
        self.db.commit()
        self.db.refresh(pipeline)

        # Mark for refresh
        self.mark_ready_for_refresh(pipeline_id)

        return self.repository.get_by_id_with_relations(pipeline_id)

    def remove_pipeline_destination(self, pipeline_id: int, destination_id: int) -> Pipeline:
        """
        Remove a destination from an existing pipeline.

        Args:
            pipeline_id: Pipeline identifier
            destination_id: Destination identifier

        Returns:
            Updated pipeline
        """
        logger.info(
            "Removing destination from pipeline",
            extra={"pipeline_id": pipeline_id, "destination_id": destination_id},
        )

        pipeline = self.repository.get_by_id(pipeline_id)

        # Check if destination exists
        existing = (
            self.db.query(PipelineDestination)
            .filter_by(pipeline_id=pipeline_id, destination_id=destination_id)
            .first()
        )
        if not existing:
             # If not found, just return current pipeline (idempotent) or raise error?
             # For idempotency, let's log and return.
             logger.warning(
                 "Destination not found in pipeline",
                 extra={"pipeline_id": pipeline_id, "destination_id": destination_id}
             )
             return self.repository.get_by_id_with_relations(pipeline_id)

        # Remove destination
        self.db.delete(existing)
        self.db.commit()
        self.db.refresh(pipeline)

        # Mark for refresh
        self.mark_ready_for_refresh(pipeline_id)

        return self.repository.get_by_id_with_relations(pipeline_id)

    def validate_target_table(self, pipeline_id: int, pipeline_destination_id: int, table_name: str) -> TableValidationResponse:
        """
        Validate table name for a destination.
        
        Args:
            pipeline_id: Pipeline identifier
            pipeline_destination_id: Pipeline Destination identifier
            table_name: Table name to validate
            
        Returns:
            Validation response
        """
        # 1. Basic format validation
        import re
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', table_name):
             return TableValidationResponse(
                 valid=False,
                 exists=False,
                 message="Table name must start with a letter or underscore and contain only alphanumeric characters and underscores."
             )
             
        # 2. Get destination
        pipeline = self.repository.get_by_id_with_relations(pipeline_id)
        pipeline_dest = next((pd for pd in pipeline.destinations if pd.id == pipeline_destination_id), None)
        
        if not pipeline_dest:
             # Try to find by destination_id directly if not found by pipeline_destination_id (sometimes frontend sends one or the other)
             # But the API arg is pipeline_destination_id. 
             # Let's double check logic. The method signature says pipeline_destination_id.
             # If checking fails, raise error.
             from app.core.exceptions import EntityNotFoundError
             raise EntityNotFoundError(entity_type="PipelineDestination", entity_id=pipeline_destination_id)
        
        destination = pipeline_dest.destination
        
        if destination.type == 'POSTGRES':
             try:
                 import psycopg2
                 # Connect to Postgres
                 conn = psycopg2.connect(
                    host=destination.config.get("host"),
                    port=destination.config.get("port"),
                    dbname=destination.config.get("database"),
                    user=destination.config.get("user"),
                    password=decrypt_value(destination.config.get("password")),
                    connect_timeout=5
                 )
                 cursor = conn.cursor()
                 
                 try:
                     # Check existence
                     # Postgres doesn't have a simple "SHOW TABLES LIKE" that works exactly the same across all versions/schemas easily
                     # querying information_schema is standard.
                     # Default schema is usually public if not specified, but let's check config
                     pg_schema = destination.config.get("schema") or "public"
                     
                     # Use SELECT 1 for better compatibility
                     query = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = %s)"
                     cursor.execute(query, (pg_schema, table_name))
                     result = cursor.fetchone()
                     logger.error(f"Table '{table_name}' exists: {result}")
                     exists = result[0] if result else False
                     logger.error(f"Table '{table_name}' exists: {exists}")
                     if exists:
                         return TableValidationResponse(
                             valid=True,
                             exists=True,
                             message=f"Table '{table_name}' already exists in schema '{pg_schema}'. It will be used as target."
                         )
                     else:
                         return TableValidationResponse(
                             valid=False,
                             exists=False,
                             message=f"Table '{table_name}' does not exist in schema '{pg_schema}' and will be created."
                         )
                 finally:
                     cursor.close()
                     conn.close()
             except Exception as e:
                 logger.error(f"Failed to validate Postgres table: {e}")
                 return TableValidationResponse(
                     valid=False, # If we can't connect, can we validate? Maybe allow it if it's just connectivity issue? 
                                  # Ideally we fail validation if we can't check.
                     exists=False,
                     message=f"Failed to validate against Postgres destination: {str(e)}"
                 )

        if destination.type != 'SNOWFLAKE':
             # For others, we might just check regex for now
              return TableValidationResponse(
                 valid=True,
                 exists=False,
                 message="Validation only fully supported for Snowflake and Postgres currently. Basic syntax check passed."
             )

        # 3. Check existence in destination (Snowflake)
        try:
             conn = self._get_snowflake_connection(destination)
             cursor = conn.cursor()
             try:
                 config = destination.config
                 db = config.get("database")
                 schema = config.get("schema")
                 
                 exists = self._check_table_exists(cursor, db, schema, table_name)
                 
                 if exists:
                     return TableValidationResponse(
                         valid=True,
                         exists=True,
                         message=f"Table '{table_name}' already exists in {db}.{schema}. It will be used as target."
                     )
                 else:
                     return TableValidationResponse(
                         valid=True,
                         exists=False,
                         message=f"Table '{table_name}' is valid and will be created in {db}.{schema}."
                     )
             finally:
                 cursor.close()
                 conn.close()
        except Exception as e:
             logger.error(f"Failed to validate table name: {e}")
             return TableValidationResponse(
                 valid=False,
                 exists=False,
                 message=f"Failed to validate against destination: {str(e)}"
             )

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

        # Mark for refresh
        self.mark_ready_for_refresh(pipeline_id)

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
                pipeline.status = PipelineStatus.PAUSE.value
                self.db.commit()
                return

            # 3. Connect to Snowflake (Iterate over destinations)
            self._update_progress(progress, 20, "Initializing destinations", "IN_PROGRESS")
            
            for index, p_dest in enumerate(pipeline.destinations):
                destination = p_dest.destination
                # Only support Snowflake for now in this provisioner? 
                # User asked for Postgres later. 
                if destination.type != 'SNOWFLAKE':
                    logger.info(f"Skipping provisioning for non-Snowflake destination: {destination.name}")
                    continue

                conn = self._get_snowflake_connection(destination)
                cursor = conn.cursor()
                
                try:
                    # Set context
                    config = destination.config
                    landing_db = config.get("landing_database")
                    landing_schema = config.get("landing_schema")
                    target_db = config.get("database")
                    target_schema = config.get("schema")

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
                        
                        # Process single table using reusable method
                        self.provision_table(pipeline, destination, table, cursor, close_cursor=False)
                
                finally:
                    cursor.close()
                    conn.close()

            # 5. Finalize
            self._update_progress(progress, 100, "Initialization completed", "COMPLETED")
            pipeline.status = PipelineStatus.PAUSE.value
            self.db.commit()
                
        except Exception as e:
            logger.error(f"Pipeline initialization failed: {e}", exc_info=True)
            # Re-fetch progress attached to session if needed, but it should be attached
            try:
                 if progress:
                    self._update_progress(progress, progress.progress, "Initialization failed", "FAILED", str(e))
            except:
                 pass

    def provision_table(self, pipeline: Pipeline, destination, table_info, cursor=None, close_cursor=False) -> None:
        """
        Provision Snowflake resources for a single table.
        
        Args:
            pipeline: Pipeline entity
            destination: Destination entity (Snowflake)
            table_info: SourceTableInfo object or similar struct with table_name, schema_definition, id
            cursor: Optional existing Snowflake cursor
            close_cursor: Whether to close the cursor if it was created internally
        """
        logger.info(f"Provisioning table {table_info.table_name} for pipeline {pipeline.name} to destination {destination.name}")
        
        # Find PipelineDestination
        pipeline_dest = next((pd for pd in pipeline.destinations if pd.destination_id == destination.id), None)
        if not pipeline_dest:
             logger.error(f"PipelineDestination not found for pipeline {pipeline.id} and destination {destination.id}")
             return

        # Get or Create PipelineDestinationTableSync
        table_name = table_info.table_name
        
        # Check if exists
        sync_record = (
            self.db.query(PipelineDestinationTableSync)
            .filter_by(pipeline_destination_id=pipeline_dest.id, table_name=table_name)
            .first()
        )
        
        if not sync_record:
            sync_record = PipelineDestinationTableSync(
                pipeline_destination_id=pipeline_dest.id,
                table_name=table_name,
                table_name_target=table_name, # Default target name same as source
                is_exists_table_landing=False,
                is_exists_stream=False,
                is_exists_task=False,
                is_exists_table_destination=False
            )
            self.db.add(sync_record)
            self.db.flush() # Flush to get ID if needed, though we operate on object
        
        conn = None
        if cursor is None:
            conn = self._get_snowflake_connection(destination)
            cursor = conn.cursor()
            close_cursor = True
            
        try:
            config = destination.config
            target_db = config.get("database")
            target_schema = config.get("schema")
            landing_db = config.get("landing_database")
            landing_schema = config.get("landing_schema")
            
            # Handle different object structures (SourceTableInfo vs Pydantic model)
            if isinstance(table_info, dict):
                 columns = table_info['schema_definition']
                 table_id = table_info['id']
            else:
                 columns = getattr(table_info, 'schema_definition', None)

            # Ensure we get the columns correctly, handling potential alias or missing fields
            if not columns and hasattr(table_info, 'schema_table'):
                # Fallback to schema_table if schema_definition is missing/empty
                st = getattr(table_info, 'schema_table')
                if isinstance(st, list):
                    columns = st
                elif isinstance(st, dict):
                    columns = list(st.values())

            # Final validation
            if not columns:
                logger.error(f"Table {table_name} has no schema definition (columns). Skipping provisioning.")
                raise ValueError(f"Table {table_name} has no columns defined. Please refresh source metadata.")

            # A. Landing Table
            if not sync_record.is_exists_table_landing:
                landing_table = f"LANDING_{table_name}"
                landing_ddl = self._generate_landing_ddl(landing_db, landing_schema, landing_table, columns)
                cursor.execute(landing_ddl)
                sync_record.is_exists_table_landing = True
            
            # B. Stream
            if not sync_record.is_exists_stream:
                landing_table = f"LANDING_{table_name}" # Reconstruct name just in case
                stream_name = f"STREAM_{landing_table}"
                stream_ddl = f"CREATE OR REPLACE STREAM {landing_db}.{landing_schema}.{stream_name} ON TABLE {landing_db}.{landing_schema}.{landing_table}"
                cursor.execute(stream_ddl)
                sync_record.is_exists_stream = True
            
            # C. Destination Table
            target_table = table_name
            # Check if table already exists (if flag is false, double check DB)
            if not sync_record.is_exists_table_destination:
                if self._check_table_exists(cursor, target_db, target_schema, target_table):
                    logger.info(f"Target table {target_db}.{target_schema}.{target_table} already exists, skipping creation.")
                    sync_record.is_exists_table_destination = True
                else:
                    logger.info(f"Creating target table {target_db}.{target_schema}.{target_table}")
                    target_ddl = self._generate_target_ddl(target_db, target_schema, target_table, columns)
                    cursor.execute(target_ddl)
                    sync_record.is_exists_table_destination = True
            
            # D. Merge Task
            if not sync_record.is_exists_task:
                landing_table = f"LANDING_{table_name}"
                stream_name = f"STREAM_{landing_table}"
                target_table = table_name
                
                task_name = f"TASK_MERGE_{table_name}"
                task_ddl = self._generate_merge_task_ddl(
                    pipeline, destination,
                    landing_db, landing_schema, landing_table,
                    stream_name,
                    target_db, target_schema, target_table,
                    columns
                )
                cursor.execute(task_ddl)
                cursor.execute(f"ALTER TASK {landing_db}.{landing_schema}.{task_name} RESUME")
                sync_record.is_exists_task = True

            self.db.commit()

        finally:
            if close_cursor:
                cursor.close()
                if conn:
                    conn.close()

    def _check_table_exists(self, cursor, db, schema, table_name) -> bool:
        """Check if a table exists in Snowflake."""
        try:
            # Use SHOW TABLES since it's reliable for existence check
            # Note: SHOW TABLES matches roughly so we filter in python or use exact match dependent on driver behavior
            # Ideally: SHOW TABLES LIKE 'tablename' IN SCHEMA db.schema
            query = f"SHOW TABLES LIKE '{table_name}' IN SCHEMA {db}.{schema}"
            cursor.execute(query)
            
            # The result cursor will have rows if there are matches
            # We iterate to find exact match because LIKE matching can be fuzzy sometimes depending on wildcard chars
            for result in cursor:
                # result structure depends on connector, usually a tuple or dict
                # index 1 usually is 'name' in SHOW TABLES output
                if result[1] == table_name:
                    return True
            return False
            
        except Exception as e:
            logger.warning(f"Failed to check if table exists: {e}")
            return False

    def _update_progress(self, progress, percent, step, status, details=None):
        progress.progress = percent
        progress.step = step
        progress.status = status
        if details:
            progress.details = details
        self.db.commit()

    def _get_snowflake_connection(self, destination):
        config = destination.config
        private_key_str = config.get("private_key", "").strip()
        passphrase = None
        if config.get("private_key_passphrase"):
            decrypted_passphrase = decrypt_value(config.get("private_key_passphrase"))
            passphrase = decrypted_passphrase.encode()

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
            user=config.get("user"),
            account=config.get("account"),
            private_key=pkb,
            role=config.get("role"),
            warehouse=config.get("warehouse"),
            database=config.get("database"),
            schema=config.get("schema"),
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

    def _generate_merge_task_ddl(self, pipeline, destination, l_db, l_schema, l_table, stream, t_db, t_schema, t_table, columns):
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
        WAREHOUSE = {destination.config.get("warehouse")}
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
        Get data flow statistics for a pipeline, grouped by destination, source table, and target table.
        
        Args:
            pipeline_id: Pipeline identifier
            days: Number of days to look back
            
        Returns:
            List of stats per table lineage
        """
        # 1. Get Pipeline and Sync Configuration
        pipeline = self.repository.get_by_id_with_relations(pipeline_id)
        
        # Pre-fetch sync configs for mapping
        # Map: (pipeline_destination_id, source_table_name) -> { target_table: str, dest_name: str }
        sync_map = {}
        if pipeline.destinations:
            for dest in pipeline.destinations:
                for sync in dest.table_syncs:
                    # Map by sync_id if available (future), or fallback to (dest_id, table_name)
                    # For now, let's map by sync.id directly
                    sync_map[sync.id] = {
                        "target_table": sync.table_name_target,
                        "destination_name": dest.destination.name
                    }
                    # Also keep legacy map for backward compatibility or when sync_id is null
                    key = (dest.id, sync.table_name)
                    if key not in sync_map:
                         sync_map[key] = {
                            "target_table": sync.table_name_target,
                            "destination_name": dest.destination.name
                        }
        
        # 2. Daily Stats Query
        start_date = datetime.now(ZoneInfo('Asia/Jakarta')) - timedelta(days=days)
        
        daily_query = (
            self.db.query(
                DataFlowRecordMonitoring.pipeline_destination_id,
                DataFlowRecordMonitoring.pipeline_destination_table_sync_id,
                DataFlowRecordMonitoring.table_name,
                func.date_trunc('day', DataFlowRecordMonitoring.created_at).label('day'),
                func.sum(DataFlowRecordMonitoring.record_count).label('total_count')
            )
            .filter(
                DataFlowRecordMonitoring.pipeline_id == pipeline_id,
                DataFlowRecordMonitoring.created_at >= start_date
            )
            .group_by(
                DataFlowRecordMonitoring.pipeline_destination_id,
                DataFlowRecordMonitoring.pipeline_destination_table_sync_id,
                DataFlowRecordMonitoring.table_name,
                func.date_trunc('day', DataFlowRecordMonitoring.created_at)
            )
            .order_by(
                DataFlowRecordMonitoring.table_name,
                desc('day')
            )
        )
        
        daily_results = daily_query.all()
        
        # 3. Recent 5 Minutes Stats Query
        five_min_ago = datetime.now(ZoneInfo('Asia/Jakarta')) - timedelta(minutes=5)
        
        recent_query = (
            self.db.query(
                DataFlowRecordMonitoring.pipeline_destination_id,
                DataFlowRecordMonitoring.pipeline_destination_table_sync_id,
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
        
        # 4. Aggregating results
        stats_map = {}
        
        # Helper to get meta info using sync_id or fallback
        def get_meta(dest_id, sync_id, table_name):
            # 1. Try sync_id first
            if sync_id and sync_id in sync_map:
                return sync_map[sync_id]
            
            # 2. Try (dest_id, table)
            if dest_id:
                info = sync_map.get((dest_id, table_name))
                if info:
                     # Check if this info is a specific dict or just one of them?
                     # The tuple key map might be ambiguous if multiple syncs same source-dest-table (rare but possible with custom sql?)
                     # But for general case it works.
                    return info

            # Fallback
            return {
                "target_table": table_name, 
                "destination_name": "Unknown Destination"
            }

        # Unique Key generator
        def get_key(dest_id, sync_id, table):
            if sync_id:
                return f"sync_{sync_id}"
            return f"{dest_id or 'none'}_{table}"

        # Process Daily Stats
        for row in daily_results:
            key = get_key(row.pipeline_destination_id, row.pipeline_destination_table_sync_id, row.table_name)
            if key not in stats_map:
                meta = get_meta(row.pipeline_destination_id, row.pipeline_destination_table_sync_id, row.table_name)
                stats_map[key] = {
                    "pipeline_destination_id": row.pipeline_destination_id,
                    "pipeline_destination_table_sync_id": row.pipeline_destination_table_sync_id,
                    "table_name": row.table_name,
                    "target_table_name": meta["target_table"],
                    "destination_name": meta["destination_name"],
                    "daily_stats": [],
                    "recent_stats": []
                }
            
            stats_map[key]["daily_stats"].append({
                "date": row.day.isoformat(),
                "count": int(row.total_count) if row.total_count else 0
            })

        # Process Recent Stats
        for row in recent_results:
            key = get_key(row.pipeline_destination_id, row.pipeline_destination_table_sync_id, row.table_name)
            if key not in stats_map:
                 meta = get_meta(row.pipeline_destination_id, row.pipeline_destination_table_sync_id, row.table_name)
                 stats_map[key] = {
                    "pipeline_destination_id": row.pipeline_destination_id,
                    "pipeline_destination_table_sync_id": row.pipeline_destination_table_sync_id,
                    "table_name": row.table_name,
                    "target_table_name": meta["target_table"],
                    "destination_name": meta["destination_name"],
                    "daily_stats": [],
                    "recent_stats": []
                }
            
            # Ensure timestamp is timezone-aware (Asia/Jakarta)
            timestamp = row.created_at
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=ZoneInfo('Asia/Jakarta'))
                
            stats_map[key]["recent_stats"].append({
                "timestamp": timestamp.isoformat(),
                "count": row.record_count
            })
            
        return list(stats_map.values())

    def get_destination_tables(self, pipeline_id: int, pipeline_destination_id: int) -> List[dict]:
        """
        Get tables available for sync with current configuration using Left Join.

        Args:
            pipeline_id: Pipeline identifier
            pipeline_destination_id: Pipeline destination identifier

        Returns:
            List of tables with sync info
        """
        from app.domain.models.table_metadata import TableMetadata
        from app.domain.models.pipeline import PipelineDestination, PipelineDestinationTableSync
        from app.domain.schemas.pipeline import (
            TableWithSyncInfoResponse,
            ColumnSchemaResponse,
            PipelineDestinationTableSyncResponse,
        )
        from app.core.exceptions import EntityNotFoundError

        # Get pipeline to verify it exists and get source_id
        pipeline = self.repository.get_by_id_with_relations(pipeline_id)

        # Verify destination exists for this pipeline
        pipeline_dest_exists = (
            self.db.query(PipelineDestination)
            .filter_by(id=pipeline_destination_id, pipeline_id=pipeline_id)
            .first()
        )
        if not pipeline_dest_exists:
             raise EntityNotFoundError(
                entity_type="PipelineDestination", entity_id=pipeline_destination_id
            )

        # 1. Get List of all tables from source metadata
        tm_repo = TableMetadataRepository(self.db)
        all_tables_meta = tm_repo.get_by_source_id(pipeline.source_id)
        
        # 2. Get existing sync configurations for this destination
        syncs = (
            self.db.query(PipelineDestinationTableSync)
            .filter_by(pipeline_destination_id=pipeline_destination_id)
            .all()
        )
        from collections import defaultdict
        syncs_map = defaultdict(list)
        for s in syncs:
            syncs_map[s.table_name].append(s)

        response_list = []
        for table_meta in all_tables_meta:
            # Parse schema
            columns = []
            if table_meta.schema_table:
                # Handle both list (older format) and dict (newer format) schemas
                schema_items = table_meta.schema_table
                if isinstance(schema_items, dict):
                    schema_items = schema_items.values()
                
                for col in schema_items:
                    if isinstance(col, dict):
                        columns.append(ColumnSchemaResponse(
                            column_name=col.get("column_name", ""),
                            data_type=col.get("real_data_type") or col.get("data_type", ""),
                            real_data_type=col.get("real_data_type"),
                            is_nullable=col.get("is_nullable") in [True, "YES"],
                            is_primary_key=col.get("is_primary_key", False),
                            has_default=col.get("has_default", False),
                            default_value=str(col.get("default_value")) if col.get("default_value") is not None else None,
                            numeric_scale=col.get("numeric_scale"),
                            numeric_precision=col.get("numeric_precision"),
                        ))
                    elif isinstance(col, str):
                        # Handle case where schema might be list of strings logic
                        columns.append(ColumnSchemaResponse(
                            column_name=col,
                            data_type="UNKNOWN",
                            is_nullable=True,
                            is_primary_key=False,
                        ))

            # Convert sync configs (list)
            current_syncs = syncs_map[table_meta.table_name]
            sync_configs_response = [
                PipelineDestinationTableSyncResponse.from_orm(s) for s in current_syncs
            ]

            response_list.append(TableWithSyncInfoResponse(
                table_name=table_meta.table_name,
                columns=columns,
                sync_configs=sync_configs_response,
                is_exists_table_landing=any(s.is_exists_table_landing for s in current_syncs),
                is_exists_stream=any(s.is_exists_stream for s in current_syncs),
                is_exists_task=any(s.is_exists_task for s in current_syncs),
                is_exists_table_destination=any(s.is_exists_table_destination for s in current_syncs),
            ))

        return [r.dict() for r in response_list]

    def save_table_sync(
        self, pipeline_id: int, pipeline_destination_id: int, table_sync_data
    ) -> "PipelineDestinationTableSync":
        """
        Create or update table sync configuration.

        Args:
            pipeline_id: Pipeline identifier
            pipeline_destination_id: Pipeline destination identifier
            table_sync_data: Table sync configuration

        Returns:
            Created/updated table sync
        """
        from app.domain.models.pipeline import PipelineDestinationTableSync
        from app.core.exceptions import EntityNotFoundError

        # Validate pipeline destination exists
        pipeline_dest = (
            self.db.query(PipelineDestination)
            .filter_by(id=pipeline_destination_id, pipeline_id=pipeline_id)
            .first()
        )
        if not pipeline_dest:
            raise EntityNotFoundError(
                entity_type="PipelineDestination", entity_id=pipeline_destination_id
            )

        if table_sync_data.id:
            # Update specific existing sync
            existing = (
                self.db.query(PipelineDestinationTableSync)
                .filter_by(
                    id=table_sync_data.id,
                    pipeline_destination_id=pipeline_destination_id
                )
                .first()
            )
            if not existing:
                 raise EntityNotFoundError(
                    entity_type="PipelineDestinationTableSync", entity_id=table_sync_data.id
                )
            
            # Verify table name matches (optional safety check)
            if existing.table_name != table_sync_data.table_name:
                # Should we allow changing source table? Probably not for a sync object.
                pass

            existing.custom_sql = table_sync_data.custom_sql
            existing.filter_sql = table_sync_data.filter_sql
            if table_sync_data.table_name_target:
                existing.table_name_target = table_sync_data.table_name_target
            
            self.db.commit()
            self.db.refresh(existing)
            
            # Mark for refresh
            self.mark_ready_for_refresh(pipeline_id)
            
            return existing
        else:
            # Create NEW sync (Branch)
            # Check if there is already a sync for this table with same target?
            # Or just allow multiple. We should probably uniqueness on (pipeline_destination_id, table_name, table_name_target)
            target_name = table_sync_data.table_name_target or table_sync_data.table_name
            
            # Optional: Check uniqueness of target for this source
            # ...

            new_sync = PipelineDestinationTableSync(
                pipeline_destination_id=pipeline_destination_id,
                table_name=table_sync_data.table_name,
                table_name_target=target_name,
                custom_sql=table_sync_data.custom_sql,
                filter_sql=table_sync_data.filter_sql,
            )
            self.db.add(new_sync)
            self.db.commit()
            self.db.refresh(new_sync)
            
            # Mark for refresh
            self.mark_ready_for_refresh(pipeline_id)
            
            return new_sync

    def save_table_syncs_bulk(
        self, pipeline_id: int, pipeline_destination_id: int, bulk_request
    ) -> List["PipelineDestinationTableSync"]:
        """
        Bulk create or update table sync configurations.

        Args:
            pipeline_id: Pipeline identifier
            pipeline_destination_id: Pipeline destination identifier
            bulk_request: Bulk table sync configurations

        Returns:
            List of created/updated table syncs
        """
        results = []
        for table_sync_data in bulk_request.tables:
            result = self.save_table_sync(
                pipeline_id, pipeline_destination_id, table_sync_data
            )
            results.append(result)
        return results

    def delete_table_sync(
        self, pipeline_id: int, pipeline_destination_id: int, table_name: str
    ) -> None:
        """
        Remove table from sync configuration.

        Args:
            pipeline_id: Pipeline identifier
            pipeline_destination_id: Pipeline destination identifier
            table_name: Table name to remove
        """
        from app.domain.models.pipeline import PipelineDestinationTableSync
        from app.core.exceptions import EntityNotFoundError

        # Validate pipeline destination exists
        pipeline_dest = (
            self.db.query(PipelineDestination)
            .filter_by(id=pipeline_destination_id, pipeline_id=pipeline_id)
            .first()
        )
        if not pipeline_dest:
            raise EntityNotFoundError(
                entity_type="PipelineDestination", entity_id=pipeline_destination_id
            )

        # Find and delete
        sync = (
            self.db.query(PipelineDestinationTableSync)
            .filter_by(
                pipeline_destination_id=pipeline_destination_id, table_name=table_name
            )
            .first()
        )

        if sync:
            self.db.delete(sync)
            self.db.commit()
            
            # Mark for refresh
            self.mark_ready_for_refresh(pipeline_id)

    def delete_table_sync_by_id(
        self, pipeline_id: int, pipeline_destination_id: int, sync_config_id: int
    ) -> None:
        """
        Remove a specific table sync configuration by ID.

        Args:
            pipeline_id: Pipeline identifier
            pipeline_destination_id: Pipeline destination identifier
            sync_config_id: Sync configuration ID to remove
        """
        from app.domain.models.pipeline import PipelineDestinationTableSync
        from app.core.exceptions import EntityNotFoundError

        # Validate pipeline destination exists
        pipeline_dest = (
            self.db.query(PipelineDestination)
            .filter_by(id=pipeline_destination_id, pipeline_id=pipeline_id)
            .first()
        )
        if not pipeline_dest:
            raise EntityNotFoundError(
                entity_type="PipelineDestination", entity_id=pipeline_destination_id
            )

        # Find and delete the specific sync by ID
        sync = (
            self.db.query(PipelineDestinationTableSync)
            .filter_by(
                id=sync_config_id,
                pipeline_destination_id=pipeline_destination_id
            )
            .first()
        )

        if not sync:
            raise EntityNotFoundError(
                entity_type="PipelineDestinationTableSync", entity_id=sync_config_id
            )

        self.db.delete(sync)
        self.db.commit()

        # Mark for refresh
        self.mark_ready_for_refresh(pipeline_id)

    def init_snowflake_table(
        self, pipeline_id: int, pipeline_destination_id: int, table_name: str
    ) -> dict:
        """
        Initialize Snowflake objects for a single table.

        Creates landing table, stream, task, and target table if they don't exist.

        Args:
            pipeline_id: Pipeline identifier
            pipeline_destination_id: Pipeline destination identifier
            table_name: Table name to initialize

        Returns:
            Status of initialization
        """
        from app.domain.repositories.table_metadata_repo import TableMetadataRepository
        from app.core.exceptions import EntityNotFoundError

        logger.info(
            f"Initializing Snowflake table",
            extra={
                "pipeline_id": pipeline_id,
                "pipeline_destination_id": pipeline_destination_id,
                "table_name": table_name,
            },
        )

        # Get pipeline and destination
        pipeline = self.repository.get_by_id_with_relations(pipeline_id)

        # Find the specific pipeline destination
        pipeline_dest = None
        destination = None
        for pd in pipeline.destinations:
            if pd.id == pipeline_destination_id:
                pipeline_dest = pd
                destination = pd.destination
                break

        if not pipeline_dest or not destination:
            raise EntityNotFoundError(
                entity_type="PipelineDestination", entity_id=pipeline_destination_id
            )

        if destination.type != "SNOWFLAKE":
            return {"status": "skipped", "message": "Not a Snowflake destination"}

        # Get table metadata
        tm_repo = TableMetadataRepository(self.db)
        table_meta = tm_repo.get_by_source_and_name(pipeline.source_id, table_name)

        if not table_meta:
            raise EntityNotFoundError(entity_type="TableMetadata", entity_id=table_name)

        # Create a simple object to pass to provision_table
        class TableInfo:
            def __init__(self, meta):
                self.id = meta.id
                self.table_name = meta.table_name
                self.schema_table = meta.schema_table

        table_info = TableInfo(table_meta)

        try:
            self.provision_table(pipeline, destination, table_info)
            return {
                "status": "success",
                "message": f"Snowflake objects created for {table_name}",
            }
        except Exception as e:
            logger.error(f"Failed to initialize Snowflake table: {e}", exc_info=True)
            return {"status": "error", "message": str(e)}

