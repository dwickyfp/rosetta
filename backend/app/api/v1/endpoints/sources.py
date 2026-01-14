"""
Source endpoints.

Provides REST API for managing data sources.
"""

from typing import List

from fastapi import APIRouter, Depends, Query, status

from app.api.deps import get_source_service
from app.domain.schemas.source import (
    SourceCreate,
    SourceResponse,
    SourceUpdate,
    SourceConnectionTest,
)
from app.domain.services.source import SourceService

router = APIRouter()


@router.post(
    "",
    response_model=SourceResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create source",
    description="Create a new PostgreSQL data source configuration",
)
async def create_source(
    source_data: SourceCreate, service: SourceService = Depends(get_source_service)
) -> SourceResponse:
    """
    Create a new source.

    Args:
        source_data: Source configuration data
        service: Source service instance

    Returns:
        Created source
    """
    source = service.create_source(source_data)
    return SourceResponse.from_orm(source)


@router.get(
    "",
    response_model=List[SourceResponse],
    summary="List sources",
    description="Get a list of all configured data sources",
)
async def list_sources(
    skip: int = Query(0, ge=0, description="Number of items to skip"),
    limit: int = Query(
        100, ge=1, le=1000, description="Maximum number of items to return"
    ),
    service: SourceService = Depends(get_source_service),
) -> List[SourceResponse]:
    """
    List all sources with pagination.

    Args:
        skip: Number of sources to skip
        limit: Maximum number of sources to return
        service: Source service instance

    Returns:
        List of sources
    """
    sources = service.list_sources(skip=skip, limit=limit)
    return [SourceResponse.from_orm(s) for s in sources]


@router.get(
    "/{source_id}",
    response_model=SourceResponse,
    summary="Get source",
    description="Get a specific source by ID",
)
async def get_source(
    source_id: int, service: SourceService = Depends(get_source_service)
) -> SourceResponse:
    """
    Get source by ID.

    Args:
        source_id: Source identifier
        service: Source service instance

    Returns:
        Source details
    """
    source = service.get_source(source_id)
    return SourceResponse.from_orm(source)


@router.put(
    "/{source_id}",
    response_model=SourceResponse,
    summary="Update source",
    description="Update an existing source configuration",
)
async def update_source(
    source_id: int,
    source_data: SourceUpdate,
    service: SourceService = Depends(get_source_service),
) -> SourceResponse:
    """
    Update source.

    Args:
        source_id: Source identifier
        source_data: Source update data
        service: Source service instance

    Returns:
        Updated source
    """
    source = service.update_source(source_id, source_data)
    return SourceResponse.from_orm(source)


@router.delete(
    "/{source_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete source",
    description="Delete a source configuration",
)
async def delete_source(
    source_id: int, service: SourceService = Depends(get_source_service)
) -> None:
    """
    Delete source.

    Args:
        source_id: Source identifier
        service: Source service instance
    """
    service.delete_source(source_id)


@router.post(
    "/test_connection",
    response_model=bool,
    summary="Test connection",
    description="Test connection with provided configuration",
)
async def test_connection(
    config: SourceConnectionTest,
    service: SourceService = Depends(get_source_service),
) -> bool:
    """
    Test connection with provided configuration.

    Args:
        config: Connection configuration
        service: Source service instance

    Returns:
        True if connection is successful, False otherwise
    """
    return service.test_connection_config(config)
