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
from app.core.security import encrypt_value, decrypt_value

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
        if not destination_data.config.get("landing_database") and destination_data.config.get("database"):
            destination_data.config["landing_database"] = destination_data.config.get("database")

        if not destination_data.config.get("landing_schema") and destination_data.config.get("schema"):
            destination_data.config["landing_schema"] = destination_data.config.get("schema")

        # Encrypt sensitive fields before storing
        if "password" in destination_data.config and destination_data.config["password"]:
             destination_data.config["password"] = encrypt_value(destination_data.config["password"])
        
        if "private_key_passphrase" in destination_data.config and destination_data.config["private_key_passphrase"]:
             destination_data.config["private_key_passphrase"] = encrypt_value(destination_data.config["private_key_passphrase"])

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

        # Get existing destination to preserve config values (especially secrets)
        existing_destination = self.repository.get_by_id(destination_id)

        # Filter out None values for partial updates
        update_data = destination_data.dict(exclude_unset=True)

        # Encrypt sensitive fields if provided in update and merge with existing
        if "config" in update_data and update_data["config"]:
            new_config = update_data["config"]
            
            # Encrypt new secrets if present
            if "password" in new_config and new_config["password"]:
                new_config["password"] = encrypt_value(new_config["password"])
            if "private_key_passphrase" in new_config and new_config["private_key_passphrase"]:
                new_config["private_key_passphrase"] = encrypt_value(new_config["private_key_passphrase"])
            
            # Merge: Use old config as base, update with new config
            # This preserves secrets that were filtered out/masked in the frontend
            final_config = existing_destination.config.copy()
            final_config.update(new_config)
            
            # Update the config in update_data
            update_data["config"] = final_config

        destination = self.repository.update(destination_id, **update_data)

        logger.info(
            "Destination updated successfully", extra={"destination_id": destination.id}
        )

        return destination

# [ ... skip to test_connection ... ]

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
        if config.type == "POSTGRES":
            import psycopg2
            try:
                conn = psycopg2.connect(
                    host=config.config.get("host"),
                    port=config.config.get("port"),
                    dbname=config.config.get("database"),
                    user=config.config.get("user"),
                    password=config.config.get("password"),
                    connect_timeout=5
                )
                conn.close()
                return True
            except Exception as e:
                logger.error(
                    "Postgres connection test failed",
                    extra={"error": str(e)},
                )
                raise Exception(f"Connection failed: {str(e)}")

        # Default to SNOWFLAKE
        import snowflake.connector
        from cryptography.hazmat.backends import default_backend
        from cryptography.hazmat.primitives import serialization

        logger.info(
            "Testing connection for destination",
            extra={
                "account": config.config.get("account"),
                "user": config.config.get("user"),
            },
        )

        try:
            if not config.config.get("private_key"):
                # if password authentication
                if config.config.get("password"):
                    # Check connection with password
                    pass
                else:
                    raise ValueError("Private key or Password is required for connection test")

            conn_params = {
                "user": config.config.get("user"),
                "account": config.config.get("account"),
                "role": config.config.get("role"),
                "warehouse": config.config.get("warehouse"),
                "database": config.config.get("database"),
                "schema": config.config.get("schema"),
                "client_session_keep_alive": False,
                "application": "Rosetta_ETL"
            }

            # Handle Private Key Auth
            if config.config.get("private_key"):
                # Clean private key string
                private_key_str = config.config.get("private_key", "").strip()
                if "\\n" in private_key_str:
                    private_key_str = private_key_str.replace("\\n", "\n")
                
                # Handle passphrase
                passphrase = None
                if config.config.get("private_key_passphrase"):
                    passphrase = config.config.get("private_key_passphrase").encode()

                try:
                    # Load private key
                    p_key = serialization.load_pem_private_key(
                        private_key_str.encode(),
                        password=passphrase,
                        backend=default_backend(),
                    )
                except ValueError as ve:
                    logger.error(f"Failed to load private key: {ve}")
                    if "Bad decrypt" in str(ve):
                         raise ValueError("Invalid Private Key Passphrase.")
                    raise ValueError("Invalid Private Key format.")

                pkb = p_key.private_bytes(
                    encoding=serialization.Encoding.DER,
                    format=serialization.PrivateFormat.PKCS8,
                    encryption_algorithm=serialization.NoEncryption(),
                )
                conn_params["private_key"] = pkb
            
            # Handle Password Auth
            elif config.config.get("password"):
                 conn_params["password"] = config.config.get("password")

            # Connect to Snowflake
            ctx = snowflake.connector.connect(**conn_params)

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
