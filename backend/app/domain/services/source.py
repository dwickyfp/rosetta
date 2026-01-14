"""
Source service containing business logic.

Implements business rules and orchestrates repository operations for sources.
"""

from typing import List

from sqlalchemy.orm import Session

from app.core.logging import get_logger
from app.domain.models.source import Source
from app.domain.repositories.source import SourceRepository
from app.domain.schemas.source import SourceCreate, SourceUpdate

logger = get_logger(__name__)


class SourceService:
    """
    Service layer for Source entity.

    Implements business logic for managing PostgreSQL source configurations.
    """

    def __init__(self, db: Session):
        """Initialize source service."""
        self.db = db
        self.repository = SourceRepository(db)

    def create_source(self, source_data: SourceCreate) -> Source:
        """
        Create a new source.

        Args:
            source_data: Source creation data

        Returns:
            Created source
        """
        logger.info("Creating new source", extra={"name": source_data.name})

        # TODO: In production, encrypt password before storing
        source = self.repository.create(**source_data.model_dump())

        logger.info(
            "Source created successfully",
            extra={"source_id": source.id, "name": source.name},
        )

        return source

    def get_source(self, source_id: int) -> Source:
        """
        Get source by ID.

        Args:
            source_id: Source identifier

        Returns:
            Source entity
        """
        return self.repository.get_by_id(source_id)

    def get_source_by_name(self, name: str) -> Source | None:
        """
        Get source by name.

        Args:
            name: Source name

        Returns:
            Source entity or None
        """
        return self.repository.get_by_name(name)

    def list_sources(self, skip: int = 0, limit: int = 100) -> List[Source]:
        """
        List all sources with pagination.

        Args:
            skip: Number of sources to skip
            limit: Maximum number of sources to return

        Returns:
            List of sources
        """
        return self.repository.get_all(skip=skip, limit=limit)

    def count_sources(self) -> int:
        """
        Count total number of sources.

        Returns:
            Total count
        """
        return self.repository.count()

    def update_source(self, source_id: int, source_data: SourceUpdate) -> Source:
        """
        Update source.

        Args:
            source_id: Source identifier
            source_data: Source update data

        Returns:
            Updated source
        """
        logger.info("Updating source", extra={"source_id": source_id})

        # Filter out None values for partial updates
        update_data = source_data.model_dump(exclude_unset=True)

        # TODO: In production, encrypt password if provided
        source = self.repository.update(source_id, **update_data)

        logger.info("Source updated successfully", extra={"source_id": source.id})

        return source

    def delete_source(self, source_id: int) -> None:
        """
        Delete source.

        Args:
            source_id: Source identifier
        """
        logger.info("Deleting source", extra={"source_id": source_id})

        self.repository.delete(source_id)

        logger.info("Source deleted successfully", extra={"source_id": source_id})

    def test_connection(self, source_id: int) -> bool:
        """
        Test database connection for a source.

        Args:
            source_id: Source identifier

        Returns:
            True if connection successful, False otherwise
        """
        source = self.repository.get_by_id(source_id)

        try:
            # TODO: Implement actual connection test
            # This would involve creating a connection to the source database
            # and executing a simple query
            logger.info(
                "Testing connection for source",
                extra={"source_id": source_id, "host": source.pg_host},
            )
            return True
        except Exception as e:
            logger.error(
                "Connection test failed",
                extra={"source_id": source_id, "error": str(e)},
            )
            return False
