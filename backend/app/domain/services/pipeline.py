"""
Pipeline service containing business logic.

Implements business rules and orchestrates repository operations for pipelines.
"""

from typing import List

from sqlalchemy.orm import Session

from app.core.logging import get_logger
from app.domain.models.pipeline import Pipeline, PipelineMetadata, PipelineStatus
from app.domain.repositories.pipeline import PipelineRepository
from app.domain.schemas.pipeline import PipelineCreate, PipelineUpdate

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

        # Create pipeline with metadata using repository method
        pipeline = self.repository.create_with_metadata(**pipeline_data.model_dump())

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
                "fields": pipeline_data.model_dump(exclude_unset=True),
            },
        )

        # Get existing pipeline to ensure it exists
        pipeline = self.repository.get_by_id(pipeline_id)

        # Update pipeline
        updated_pipeline = self.repository.update(
            pipeline_id, **pipeline_data.model_dump(exclude_unset=True)
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
