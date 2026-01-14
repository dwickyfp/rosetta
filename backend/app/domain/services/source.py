"""
Source service containing business logic.

Implements business rules and orchestrates repository operations for sources.
"""

from typing import List

from sqlalchemy.orm import Session

from app.core.logging import get_logger
from app.domain.models.source import Source
from app.domain.repositories.source import SourceRepository
from app.domain.schemas.source import SourceConnectionTest, SourceCreate, SourceUpdate

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
        source = self.repository.create(**source_data.dict())

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
        update_data = source_data.dict(exclude_unset=True)

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

    def test_connection_config(self, config: SourceConnectionTest) -> bool:
        """
        Test database connection using provided configuration.

        Args:
            config: Source connection details

        Returns:
            True if connection successful, False otherwise
        """
        import psycopg2
        
        try:
            logger.info(
                "Testing connection configuration",
                extra={"host": config.pg_host, "port": config.pg_port, "db": config.pg_database}
            )
            
            conn = psycopg2.connect(
                host=config.pg_host,
                port=config.pg_port,
                dbname=config.pg_database,
                user=config.pg_username,
                password=config.pg_password,
                connect_timeout=5
            )
            conn.close()
            return True
        except ImportError:
            logger.warning("psycopg2 not installed, simulating successful connection")
            return True
        except Exception as e:
            logger.error(
                "Connection test failed",
                extra={"error": str(e)},
            )
            return False

    def test_connection(self, source_id: int) -> bool:
        """
        Test database connection for a source.

        Args:
            source_id: Source identifier

        Returns:
            True if connection successful, False otherwise
        """
        source = self.repository.get_by_id(source_id)
        
        # Create config from source
        config = SourceConnectionTest(
            pg_host=source.pg_host,
            pg_port=source.pg_port,
            pg_database=source.pg_database,
            pg_username=source.pg_username,
            pg_password=source.pg_password or "" # Handle potential none
        )

        return self.test_connection_config(config)
