"""
Table Sync endpoints.

Provides REST API for managing table synchronization configurations.
"""

from typing import List

from fastapi import APIRouter, Depends, Path, status, BackgroundTasks, HTTPException
from sqlalchemy.orm import Session

from app.api.deps import get_db, get_pipeline_service
from app.domain.schemas.pipeline import (
    TableSyncCreateRequest,
    TableSyncBulkRequest,
    TableWithSyncInfoResponse,
    PipelineDestinationTableSyncResponse,
)
from app.domain.services.pipeline import PipelineService
from app.domain.models.pipeline import PipelineDestination, PipelineDestinationTableSync
from app.domain.repositories.table_metadata_repo import TableMetadataRepository
from app.core.exceptions import EntityNotFoundError

router = APIRouter()


@router.get(
    "/{pipeline_id}/destinations/{pipeline_destination_id}/tables",
    response_model=List[TableWithSyncInfoResponse],
    summary="Get tables for sync",
    description="Get list of available tables with their sync configuration",
)
async def get_destination_tables(
    pipeline_id: int = Path(..., description="Pipeline ID"),
    pipeline_destination_id: int = Path(..., description="Pipeline Destination ID"),
    service: PipelineService = Depends(get_pipeline_service),
) -> List[TableWithSyncInfoResponse]:
    """
    Get tables available for sync with current configuration.

    Args:
        pipeline_id: Pipeline identifier
        pipeline_destination_id: Pipeline destination identifier
        service: Pipeline service instance

    Returns:
        List of tables with sync info
    """
    return service.get_destination_tables(pipeline_id, pipeline_destination_id)


@router.post(
    "/{pipeline_id}/destinations/{pipeline_destination_id}/tables",
    response_model=PipelineDestinationTableSyncResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create or update table sync",
    description="Create or update table synchronization configuration",
)
async def save_table_sync(
    pipeline_id: int = Path(..., description="Pipeline ID"),
    pipeline_destination_id: int = Path(..., description="Pipeline Destination ID"),
    table_sync: TableSyncCreateRequest = ...,
    service: PipelineService = Depends(get_pipeline_service),
) -> PipelineDestinationTableSyncResponse:
    """
    Create or update table sync configuration.

    Args:
        pipeline_id: Pipeline identifier
        pipeline_destination_id: Pipeline destination identifier
        table_sync: Table sync configuration
        service: Pipeline service instance

    Returns:
        Created/updated table sync configuration
    """
    return service.save_table_sync(pipeline_id, pipeline_destination_id, table_sync)


@router.post(
    "/{pipeline_id}/destinations/{pipeline_destination_id}/tables/bulk",
    response_model=List[PipelineDestinationTableSyncResponse],
    status_code=status.HTTP_201_CREATED,
    summary="Bulk save table syncs",
    description="Create or update multiple table synchronization configurations",
)
async def save_table_syncs_bulk(
    pipeline_id: int = Path(..., description="Pipeline ID"),
    pipeline_destination_id: int = Path(..., description="Pipeline Destination ID"),
    bulk_request: TableSyncBulkRequest = ...,
    service: PipelineService = Depends(get_pipeline_service),
) -> List[PipelineDestinationTableSyncResponse]:
    """
    Bulk create or update table sync configurations.

    Args:
        pipeline_id: Pipeline identifier
        pipeline_destination_id: Pipeline destination identifier
        bulk_request: Bulk table sync configurations
        service: Pipeline service instance

    Returns:
        List of created/updated table sync configurations
    """
    return service.save_table_syncs_bulk(pipeline_id, pipeline_destination_id, bulk_request)


@router.delete(
    "/{pipeline_id}/destinations/{pipeline_destination_id}/tables/{table_name}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Remove table from sync",
    description="Remove a table from synchronization",
)
async def delete_table_sync(
    pipeline_id: int = Path(..., description="Pipeline ID"),
    pipeline_destination_id: int = Path(..., description="Pipeline Destination ID"),
    table_name: str = Path(..., description="Table name to remove"),
    service: PipelineService = Depends(get_pipeline_service),
) -> None:
    """
    Remove table from sync configuration.

    Args:
        pipeline_id: Pipeline identifier
        pipeline_destination_id: Pipeline destination identifier
        table_name: Table name to remove
        service: Pipeline service instance
    """
    service.delete_table_sync(pipeline_id, pipeline_destination_id, table_name)


@router.post(
    "/{pipeline_id}/destinations/{pipeline_destination_id}/tables/{table_name}/init",
    response_model=dict,
    summary="Initialize Snowflake objects",
    description="Create landing table, stream, task, and target table in Snowflake",
)
async def init_snowflake_table(
    pipeline_id: int = Path(..., description="Pipeline ID"),
    pipeline_destination_id: int = Path(..., description="Pipeline Destination ID"),
    table_name: str = Path(..., description="Table name to initialize"),
    background_tasks: BackgroundTasks = None,
    service: PipelineService = Depends(get_pipeline_service),
) -> dict:
    """
    Initialize Snowflake objects for a table.

    Creates landing table, stream, task, and target table if they don't exist.

    Args:
        pipeline_id: Pipeline identifier
        pipeline_destination_id: Pipeline destination identifier
        table_name: Table name to initialize
        background_tasks: Background tasks for async processing
        service: Pipeline service instance

    Returns:
        Status of initialization
    """
    return service.init_snowflake_table(pipeline_id, pipeline_destination_id, table_name)
