"""
Destination Pydantic schemas for request/response validation.

Defines schemas for creating, updating, and retrieving destination configurations.
"""

from typing import Optional

from pydantic import Field, validator

from app.domain.schemas.common import BaseSchema, TimestampSchema


class DestinationBase(BaseSchema):
    """Base destination schema with common fields."""

    name: str = Field(
        ...,
        min_length=1,
        max_length=255,
        description="Unique destination name",
        examples=["snowflake-prod", "analytics-warehouse"],
    )


class DestinationCreate(DestinationBase):
    """
    Schema for creating a new destination.

    Requires Snowflake connection details.
    """

    snowflake_account: str | None = Field(
        default=None,
        max_length=255,
        description="Snowflake account identifier",
        examples=["xy12345", "xy12345.us-east-1"],
    )
    snowflake_user: str | None = Field(
        default=None,
        max_length=255,
        description="Snowflake username",
        examples=["ETL_USER", "admin"],
    )
    snowflake_database: str | None = Field(
        default=None,
        max_length=255,
        description="Snowflake database name",
        examples=["ANALYTICS", "PRODUCTION"],
    )
    snowflake_schema: str | None = Field(
        default=None,
        max_length=255,
        description="Snowflake schema name",
        examples=["PUBLIC", "RAW_DATA"],
    )
    snowflake_landing_database: str | None = Field(
        default=None,
        max_length=255,
        description="Snowflake landing database name",
        examples=["LANDING", "RAW_LANDING"],
    )
    snowflake_landing_schema: str | None = Field(
        default=None,
        max_length=255,
        description="Snowflake landing schema name",
        examples=["PUBLIC", "RAW"],
    )
    snowflake_role: str | None = Field(
        default=None,
        max_length=255,
        description="Snowflake role name",
        examples=["ACCOUNTADMIN", "SYSADMIN"],
    )
    snowflake_private_key: str | None = Field(
        default=None,
        description="Snowflake private key content (PEM)",
        examples=["-----BEGIN PRIVATE KEY-----\nMII..."],
    )
    snowflake_private_key_passphrase: str | None = Field(
        default=None,
        max_length=255,
        description="Private key passphrase (will be encrypted)",
        examples=["MySecurePassphrase123!"],
    )
    snowflake_warehouse: str | None = Field(
        default=None,
        max_length=255,
        description="Snowflake warehouse name",
        examples=["COMPUTE_WH"],
    )

    @validator("name")
    def validate_name(cls, v: str) -> str:
        """Validate destination name format."""
        if not v.replace("-", "").replace("_", "").isalnum():
            raise ValueError(
                "Destination name must contain only alphanumeric characters, "
                "hyphens, and underscores"
            )
        return v.lower()

    @validator("snowflake_account")
    def validate_snowflake_account(cls, v: str | None) -> str | None:
        """Validate Snowflake account format."""
        if v is not None:
            # Basic validation - can be enhanced
            if not v.replace(".", "").replace("-", "").isalnum():
                raise ValueError(
                    "Snowflake account must contain only alphanumeric characters, "
                    "dots, and hyphens"
                )
        return v

    class Config:
        schema_extra = {
            "example": {
                "name": "snowflake-production",
                "snowflake_account": "xy12345.us-east-1",
                "snowflake_user": "ETL_USER",
                "snowflake_database": "ANALYTICS",
                "snowflake_schema": "RAW_DATA",
                "snowflake_role": "SYSADMIN",
                "snowflake_warehouse": "COMPUTE_WH",
                "snowflake_private_key_passphrase": "MySecurePassphrase123!",
            }
        }


class DestinationUpdate(BaseSchema):
    """
    Schema for updating an existing destination.

    All fields are optional to support partial updates.
    """

    name: str | None = Field(
        default=None,
        min_length=1,
        max_length=255,
        description="Unique destination name",
    )
    snowflake_account: str | None = Field(
        default=None, max_length=255, description="Snowflake account identifier"
    )
    snowflake_user: str | None = Field(
        default=None, max_length=255, description="Snowflake username"
    )
    snowflake_database: str | None = Field(
        default=None, max_length=255, description="Snowflake database name"
    )
    snowflake_schema: str | None = Field(
        default=None, max_length=255, description="Snowflake schema name"
    )
    snowflake_landing_database: str | None = Field(
        default=None, max_length=255, description="Snowflake landing database name"
    )
    snowflake_landing_schema: str | None = Field(
        default=None, max_length=255, description="Snowflake landing schema name"
    )
    snowflake_role: str | None = Field(
        default=None, max_length=255, description="Snowflake role name"
    )
    snowflake_private_key: str | None = Field(
        default=None, description="Snowflake private key content (PEM)"
    )
    snowflake_private_key_passphrase: str | None = Field(
        default=None,
        max_length=255,
        description="Private key passphrase (will be encrypted)",
    )
    snowflake_warehouse: str | None = Field(
        default=None, max_length=255, description="Snowflake warehouse name"
    )

    @validator("name")
    def validate_name(cls, v: str | None) -> str | None:
        """Validate destination name format."""
        if v is not None:
            if not v.replace("-", "").replace("_", "").isalnum():
                raise ValueError(
                    "Destination name must contain only alphanumeric characters, "
                    "hyphens, and underscores"
                )
            return v.lower()
        return v


class DestinationResponse(DestinationBase, TimestampSchema):
    """
    Schema for destination API responses.

    Includes all destination details (except sensitive passphrase).
    """

    id: int = Field(..., description="Unique destination identifier", examples=[1, 42])
    snowflake_account: str | None = Field(
        default=None, description="Snowflake account identifier"
    )
    snowflake_user: str | None = Field(default=None, description="Snowflake username")
    snowflake_database: str | None = Field(
        default=None, description="Snowflake database name"
    )
    snowflake_schema: str | None = Field(
        default=None, description="Snowflake schema name"
    )
    snowflake_landing_database: str | None = Field(
        default=None, description="Snowflake landing database name"
    )
    snowflake_landing_schema: str | None = Field(
        default=None, description="Snowflake landing schema name"
    )
    snowflake_role: str | None = Field(default=None, description="Snowflake role name")
    snowflake_private_key: str | None = Field(
        default=None, description="Snowflake private key content"
    )
    snowflake_warehouse: str | None = Field(
        default=None, description="Snowflake warehouse name"
    )

    class Config:
        orm_mode = True
        fields = {"snowflake_private_key_passphrase": {"exclude": True}}
        schema_extra = {
            "example": {
                "id": 1,
                "name": "snowflake-production",
                "snowflake_account": "xy12345.us-east-1",
                "snowflake_user": "ETL_USER",
                "snowflake_database": "ANALYTICS",
                "snowflake_schema": "RAW_DATA",
                "snowflake_role": "SYSADMIN",
                "snowflake_warehouse": "COMPUTE_WH",
                "created_at": "2024-01-01T00:00:00Z",
                "updated_at": "2024-01-01T00:00:00Z",
            }
        }
