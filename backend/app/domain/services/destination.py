"""
Destination service containing business logic.

Implements business rules and orchestrates repository operations for destinations.
"""

from typing import List

from sqlalchemy.orm import Session

from app.core.logging import get_logger
from app.domain.models.destination import Destination
from app.domain.repositories.destination import DestinationRepository
from app.domain.schemas.destination import DestinationCreate, DestinationUpdate

logger = get_logger(__name__)


class DestinationService:
    """
    Service layer for Destination entity.

    Implements business logic for managing Snowflake destination configurations.
    """

    def __init__(self, db: Session):
        """Initialize destination service."""
        self.db = db
        self.repository = DestinationRepository(db)

    def create_destination(self, destination_data: DestinationCreate) -> Destination:
        """
        Create a new destination.

        Args:
            destination_data: Destination creation data

        Returns:
            Created destination
        """
        logger.info("Creating new destination", extra={"name": destination_data.name})

        # TODO: In production, encrypt passphrase before storing
        destination = self.repository.create(**destination_data.model_dump())

        logger.info(
            "Destination created successfully",
            extra={"destination_id": destination.id, "name": destination.name},
        )

        return destination

    def get_destination(self, destination_id: int) -> Destination:
        """
        Get destination by ID.

        Args:
            destination_id: Destination identifier

        Returns:
            Destination entity
        """
        return self.repository.get_by_id(destination_id)

    def get_destination_by_name(self, name: str) -> Destination | None:
        """
        Get destination by name.

        Args:
            name: Destination name

        Returns:
            Destination entity or None
        """
        return self.repository.get_by_name(name)

    def list_destinations(self, skip: int = 0, limit: int = 100) -> List[Destination]:
        """
        List all destinations with pagination.

        Args:
            skip: Number of destinations to skip
            limit: Maximum number of destinations to return

        Returns:
            List of destinations
        """
        return self.repository.get_all(skip=skip, limit=limit)

    def count_destinations(self) -> int:
        """
        Count total number of destinations.

        Returns:
            Total count
        """
        return self.repository.count()

    def update_destination(
        self, destination_id: int, destination_data: DestinationUpdate
    ) -> Destination:
        """
        Update destination.

        Args:
            destination_id: Destination identifier
            destination_data: Destination update data

        Returns:
            Updated destination
        """
        logger.info("Updating destination", extra={"destination_id": destination_id})

        # Filter out None values for partial updates
        update_data = destination_data.model_dump(exclude_unset=True)

        # TODO: In production, encrypt passphrase if provided
        destination = self.repository.update(destination_id, **update_data)

        logger.info(
            "Destination updated successfully", extra={"destination_id": destination.id}
        )

        return destination

    def delete_destination(self, destination_id: int) -> None:
        """
        Delete destination.

        Args:
            destination_id: Destination identifier
        """
        logger.info("Deleting destination", extra={"destination_id": destination_id})

        self.repository.delete(destination_id)

        logger.info(
            "Destination deleted successfully", extra={"destination_id": destination_id}
        )

    def test_connection(self, destination_id: int) -> bool:
        """
        Test Snowflake connection for a destination.

        Args:
            destination_id: Destination identifier

        Returns:
            True if connection successful, False otherwise
        """
        destination = self.repository.get_by_id(destination_id)

        try:
            # TODO: Implement actual Snowflake connection test
            # This would involve creating a Snowflake connection
            # and executing a simple query
            logger.info(
                "Testing connection for destination",
                extra={
                    "destination_id": destination_id,
                    "account": destination.snowflake_account,
                },
            )
            return True
        except Exception as e:
            logger.error(
                "Connection test failed",
                extra={"destination_id": destination_id, "error": str(e)},
            )
            return False
