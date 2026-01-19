"""
Pipeline Pydantic schemas for request/response validation.

Defines schemas for creating, updating, and retrieving pipeline configurations.
"""

from datetime import datetime

from pydantic import Field, validator

from app.domain.models.pipeline import PipelineMetadataStatus, PipelineStatus
from app.domain.schemas.common import BaseSchema, TimestampSchema
from app.domain.schemas.destination import DestinationResponse
from app.domain.schemas.source import SourceResponse


class PipelineBase(BaseSchema):
    """Base pipeline schema with common fields."""

    name: str = Field(
        ...,
        min_length=1,
        max_length=255,
        description="Unique pipeline name",
        examples=["prod-to-snowflake", "analytics-sync"],
    )


class PipelineCreate(PipelineBase):
    """
    Schema for creating a new pipeline.

    Connects a source to a destination.
    """

    source_id: int = Field(
        ..., ge=1, description="ID of the source database", examples=[1, 42]
    )
    destination_id: int = Field(
        ..., ge=1, description="ID of the destination warehouse", examples=[1, 42]
    )
    status: PipelineStatus = Field(
        default=PipelineStatus.START,
        description="Initial pipeline status",
        examples=["START", "PAUSE"],
    )

    @validator("name")
    def validate_name(cls, v: str) -> str:
        """Validate pipeline name format."""
        if not v.replace("-", "").replace("_", "").isalnum():
            raise ValueError(
                "Pipeline name must contain only alphanumeric characters, "
                "hyphens, and underscores"
            )
        return v.lower()

    class Config:
        schema_extra = {
            "example": {
                "name": "production-to-snowflake",
                "source_id": 1,
                "destination_id": 1,
                "status": "START",
            }
        }


class PipelineUpdate(BaseSchema):
    """
    Schema for updating an existing pipeline.

    All fields are optional to support partial updates.
    """

    name: str | None = Field(
        default=None, min_length=1, max_length=255, description="Unique pipeline name"
    )
    source_id: int | None = Field(
        default=None, ge=1, description="ID of the source database"
    )
    destination_id: int | None = Field(
        default=None, ge=1, description="ID of the destination warehouse"
    )
    status: PipelineStatus | None = Field(default=None, description="Pipeline status")

    @validator("name")
    def validate_name(cls, v: str | None) -> str | None:
        """Validate pipeline name format."""
        if v is not None:
            if not v.replace("-", "").replace("_", "").isalnum():
                raise ValueError(
                    "Pipeline name must contain only alphanumeric characters, "
                    "hyphens, and underscores"
                )
            return v.lower()
        return v


class PipelineStatusUpdate(BaseSchema):
    """
    Schema for updating pipeline status.

    Used for start/pause/refresh operations.
    """

    status: PipelineStatus = Field(
        ..., description="New pipeline status", examples=["START", "PAUSE", "REFRESH"]
    )

    class Config:
        schema_extra = {"example": {"status": "START"}}


class PipelineMetadataResponse(BaseSchema):
    """
    Schema for pipeline metadata responses.

    Contains runtime status and error information.
    """

    id: int = Field(..., description="Unique metadata identifier")
    pipeline_id: int = Field(..., description="Associated pipeline ID")
    status: PipelineMetadataStatus = Field(..., description="Runtime status")
    last_error: str | None = Field(default=None, description="Last error message")
    last_error_at: datetime | None = Field(
        default=None, description="Timestamp of last error"
    )
    last_start_at: datetime | None = Field(
        default=None, description="Timestamp of last pipeline start"
    )
    created_at: datetime = Field(..., description="Metadata creation timestamp")
    updated_at: datetime = Field(..., description="Metadata last update timestamp")

    class Config:
        orm_mode = True
        schema_extra = {
            "example": {
                "id": 1,
                "pipeline_id": 1,
                "status": "RUNNING",
                "last_error": None,
                "last_error_at": None,
                "last_start_at": "2024-01-01T00:00:00Z",
                "created_at": "2024-01-01T00:00:00Z",
                "updated_at": "2024-01-01T00:00:00Z",
            }
        }



class PipelineProgressResponse(BaseSchema):
    """
    Schema for pipeline progress.
    """

    id: int = Field(..., description="Unique progress identifier")
    pipeline_id: int = Field(..., description="Associated pipeline ID")
    progress: int = Field(..., description="Progress percentage")
    step: str | None = Field(default=None, description="Current step")
    status: str = Field(..., description="Progress status")
    details: str | None = Field(default=None, description="Additional details")
    created_at: datetime = Field(..., description="Creation timestamp")
    updated_at: datetime = Field(..., description="Last update timestamp")

    class Config:
        orm_mode = True


class PipelineResponse(PipelineBase, TimestampSchema):

    """
    Schema for pipeline API responses.

    Includes full pipeline details with related source and destination.
    """

    id: int = Field(..., description="Unique pipeline identifier", examples=[1, 42])
    source_id: int = Field(..., description="ID of the source database")
    destination_id: int = Field(..., description="ID of the destination warehouse")
    status: PipelineStatus = Field(..., description="Pipeline operational status")

    # Nested relationships
    source: SourceResponse | None = Field(
        default=None, description="Source configuration details"
    )
    destination: DestinationResponse | None = Field(
        default=None, description="Destination configuration details"
    )
    pipeline_metadata: PipelineMetadataResponse | None = Field(
        default=None, description="Pipeline runtime metadata"
    )
    pipeline_progress: PipelineProgressResponse | None = Field(
        default=None, description="Pipeline initialization progress"
    )

    class Config:
        orm_mode = True
        schema_extra = {
            "example": {
                "id": 1,
                "name": "production-to-snowflake",
                "source_id": 1,
                "destination_id": 1,
                "status": "START",
                "created_at": "2024-01-01T00:00:00Z",
                "updated_at": "2024-01-01T00:00:00Z",
                "source": {
                    "id": 1,
                    "name": "production-postgres",
                    "pg_host": "postgres.example.com",
                    "pg_port": 5432,
                    "pg_database": "myapp_production",
                    "pg_username": "replication_user",
                    "publication_name": "dbz_publication",
                    "replication_id": 1,
                    "created_at": "2024-01-01T00:00:00Z",
                    "updated_at": "2024-01-01T00:00:00Z",
                },
                "destination": {
                    "id": 1,
                    "name": "snowflake-production",
                    "snowflake_account": "xy12345.us-east-1",
                    "snowflake_user": "ETL_USER",
                    "snowflake_database": "ANALYTICS",
                    "snowflake_schema": "RAW_DATA",
                    "snowflake_role": "SYSADMIN",
                    "snowflake_private_key_path": "user/snowflake_key.p8",
                    "snowflake_host": "xy12345.snowflakecomputing.com",
                    "created_at": "2024-01-01T00:00:00Z",
                    "updated_at": "2024-01-01T00:00:00Z",
                },
                "pipeline_metadata": {
                    "id": 1,
                    "pipeline_id": 1,
                    "status": "RUNNING",
                    "last_error": None,
                    "last_error_at": None,
                    "last_start_at": "2024-01-01T00:00:00Z",
                    "created_at": "2024-01-01T00:00:00Z",
                    "updated_at": "2024-01-01T00:00:00Z",
                },
                "pipeline_progress": {
                    "id": 1,
                    "pipeline_id": 1,
                    "progress": 50,
                    "step": "Creating Landing Table",
                    "status": "IN_PROGRESS",
                    "details": None,
                    "created_at": "2024-01-01T00:00:00Z",
                    "updated_at": "2024-01-01T00:00:00Z",
                },
            }
        }
