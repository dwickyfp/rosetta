"""
Destination model - Snowflake data warehouse configurations.

Represents Snowflake connection configurations for ETL destinations.
"""

from typing import TYPE_CHECKING

from sqlalchemy import Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.domain.models.base import Base, TimestampMixin

if TYPE_CHECKING:
    from app.domain.models.pipeline import Pipeline


class Destination(Base, TimestampMixin):
    """
    Snowflake destination configuration.

    Stores connection details for Snowflake data warehouses that serve
    as destinations for ETL pipelines.
    """

    __tablename__ = "destinations"
    __table_args__ = (
        UniqueConstraint("name", name="uq_destinations_name"),
        {"comment": "Snowflake destination configurations"},
    )

    # Primary Key
    id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
        comment="Unique destination identifier",
    )

    # Destination Identification
    name: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        unique=True,
        index=True,
        comment="Unique destination name",
    )

    # Snowflake Connection Details
    snowflake_account: Mapped[str | None] = mapped_column(
        String(255), nullable=True, comment="Snowflake account identifier"
    )

    snowflake_user: Mapped[str | None] = mapped_column(
        String(255), nullable=True, comment="Snowflake username"
    )

    snowflake_database: Mapped[str | None] = mapped_column(
        String(255), nullable=True, comment="Snowflake database name"
    )

    snowflake_schema: Mapped[str | None] = mapped_column(
        String(255), nullable=True, comment="Snowflake schema name"
    )

    snowflake_role: Mapped[str | None] = mapped_column(
        String(255), nullable=True, comment="Snowflake role name"
    )

    snowflake_landing_database: Mapped[str | None] = mapped_column(
        String(255), nullable=True, comment="Snowflake landing database name"
    )

    snowflake_landing_schema: Mapped[str | None] = mapped_column(
        String(255), nullable=True, comment="Snowflake landing schema name"
    )

    # Authentication
    snowflake_private_key: Mapped[str | None] = mapped_column(
        "snowflake_private_key", String, nullable=True, comment="Snowflake private key (PEM content)"
    )

    snowflake_private_key_passphrase: Mapped[str | None] = mapped_column(
        String(255), nullable=True, comment="Private key passphrase (encrypted)"
    )

    snowflake_warehouse: Mapped[str | None] = mapped_column(
        String(255), nullable=True, comment="Snowflake warehouse name"
    )

    # Relationships
    pipelines: Mapped[list["Pipeline"]] = relationship(
        "Pipeline",
        back_populates="destination",
        cascade="all, delete-orphan",
        lazy="selectin",
    )

    def __repr__(self) -> str:
        """String representation."""
        return (
            f"Destination(id={self.id}, name={self.name!r}, "
            f"account={self.snowflake_account!r})"
        )

    @property
    def connection_config(self) -> dict[str, str]:
        """
        Get Snowflake connection configuration.

        Returns dictionary with connection parameters for Snowflake client.
        """
        config = {
            "account": self.snowflake_account or "",
            "user": self.snowflake_user or "",
            "database": self.snowflake_database or "",
            "schema": self.snowflake_schema or "",
            "role": self.snowflake_role or "",
        }

        if self.snowflake_warehouse:
            config["warehouse"] = self.snowflake_warehouse

        if self.snowflake_landing_database:
            config["landing_database"] = self.snowflake_landing_database

        if self.snowflake_landing_schema:
            config["landing_schema"] = self.snowflake_landing_schema

        if self.snowflake_private_key:
            config["private_key"] = self.snowflake_private_key

        if self.snowflake_private_key_passphrase:
            config["private_key_passphrase"] = self.snowflake_private_key_passphrase

        return config

        if self.snowflake_private_key_passphrase:
            config["private_key_passphrase"] = self.snowflake_private_key_passphrase

        return config
