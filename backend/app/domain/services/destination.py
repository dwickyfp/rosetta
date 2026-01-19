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

        # Default landing configuration to standard configuration if not provided
        if not destination_data.snowflake_landing_database and destination_data.snowflake_database:
            destination_data.snowflake_landing_database = destination_data.snowflake_database

        if not destination_data.snowflake_landing_schema and destination_data.snowflake_schema:
            destination_data.snowflake_landing_schema = destination_data.snowflake_schema

        # TODO: In production, encrypt passphrase before storing
        destination = self.repository.create(**destination_data.dict())

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
        update_data = destination_data.dict(exclude_unset=True)

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

    def test_connection(self, config: DestinationCreate) -> bool:
        """
        Test Snowflake connection for a destination configuration.

        Args:
            config: Destination configuration to test

        Returns:
            True if connection successful

        Raises:
            Exception: If connection fails, with error details
        """
        import snowflake.connector
        from cryptography.hazmat.backends import default_backend
        from cryptography.hazmat.primitives import serialization

        logger.info(
            "Testing connection for destination",
            extra={
                "account": config.snowflake_account,
                "user": config.snowflake_user,
            },
        )

        try:
            if not config.snowflake_private_key:
                raise ValueError("Private key is required for connection test")

            # Clean private key string
            private_key_str = config.snowflake_private_key.strip()
            
            # Handle passphrase
            passphrase = None
            if config.snowflake_private_key_passphrase:
                passphrase = config.snowflake_private_key_passphrase.encode()

            try:
                # Load private key
                p_key = serialization.load_pem_private_key(
                    private_key_str.encode(),
                    password=passphrase,
                    backend=default_backend(),
                )
            except ValueError as ve:
                logger.error(f"Failed to load private key: {ve}")
                raise ValueError("Invalid Private Key or Passphrase. Please check your credentials.")

            pkb = p_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )

            # Connect to Snowflake
            # Note: snowflake-connector-python usually uppercases the user for JWT unless quoted.
            # We pass the parameters as is.
            ctx = snowflake.connector.connect(
                user=config.snowflake_user,
                account=config.snowflake_account,
                private_key=pkb,
                role=config.snowflake_role,
                warehouse=config.snowflake_warehouse,
                database=config.snowflake_database,
                schema=config.snowflake_schema,
                client_session_keep_alive=False,
                application="Rosetta_ETL"
            )

            # Test query
            cs = ctx.cursor()
            cs.execute("SELECT 1")
            result = cs.fetchone()
            
            cs.close()
            ctx.close()

            if result and result[0] == 1:
                return True
            return False

        except snowflake.connector.errors.ProgrammingError as pe:
             logger.error(
                "Snowflake programming error",
                extra={"error": str(pe)},
            )
             # Catch specific JWT errors to give better hints
             if "JWT token is invalid" in str(pe):
                 raise Exception("Authentication Failed: JWT token is invalid. Please check if the Public Key is correctly assigned to the user in Snowflake, and the Username matches.")
             raise pe

        except Exception as e:
            logger.error(
                "Connection test failed",
                extra={"error": str(e)},
            )
            # Re-raise with clear message if possible
            raise e
