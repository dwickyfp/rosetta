"""
Destination endpoints.

Provides REST API for managing data destinations.
"""

from typing import List

from fastapi import APIRouter, Depends, Query, status

from app.api.deps import get_destination_service
from app.domain.schemas.destination import (
    DestinationCreate,
    DestinationResponse,
    DestinationUpdate,
)
from app.domain.services.destination import DestinationService

router = APIRouter()


@router.post(
    "",
    response_model=DestinationResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create destination",
    description="Create a new Snowflake data destination configuration",
)
async def create_destination(
    destination_data: DestinationCreate,
    service: DestinationService = Depends(get_destination_service),
) -> DestinationResponse:
    """
    Create a new destination.

    Args:
        destination_data: Destination configuration data
        service: Destination service instance

    Returns:
        Created destination
    """
    destination = service.create_destination(destination_data)
    return DestinationResponse.from_orm(destination)


@router.get(
    "",
    response_model=List[DestinationResponse],
    summary="List destinations",
    description="Get a list of all configured data destinations",
)
async def list_destinations(
    skip: int = Query(0, ge=0, description="Number of items to skip"),
    limit: int = Query(
        100, ge=1, le=1000, description="Maximum number of items to return"
    ),
    service: DestinationService = Depends(get_destination_service),
) -> List[DestinationResponse]:
    """
    List all destinations with pagination.

    Args:
        skip: Number of destinations to skip
        limit: Maximum number of destinations to return
        service: Destination service instance

    Returns:
        List of destinations
    """
    destinations = service.list_destinations(skip=skip, limit=limit)
    return [DestinationResponse.from_orm(d) for d in destinations]


@router.get(
    "/{destination_id}",
    response_model=DestinationResponse,
    summary="Get destination",
    description="Get a specific destination by ID",
)
async def get_destination(
    destination_id: int, service: DestinationService = Depends(get_destination_service)
) -> DestinationResponse:
    """
    Get destination by ID.

    Args:
        destination_id: Destination identifier
        service: Destination service instance

    Returns:
        Destination details
    """
    destination = service.get_destination(destination_id)
    return DestinationResponse.from_orm(destination)


@router.put(
    "/{destination_id}",
    response_model=DestinationResponse,
    summary="Update destination",
    description="Update an existing destination configuration",
)
async def update_destination(
    destination_id: int,
    destination_data: DestinationUpdate,
    service: DestinationService = Depends(get_destination_service),
) -> DestinationResponse:
    """
    Update destination.

    Args:
        destination_id: Destination identifier
        destination_data: Destination update data
        service: Destination service instance

    Returns:
        Updated destination
    """
    destination = service.update_destination(destination_id, destination_data)
    return DestinationResponse.from_orm(destination)


@router.delete(
    "/{destination_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete destination",
    description="Delete a destination configuration",
)
async def delete_destination(
    destination_id: int, service: DestinationService = Depends(get_destination_service)
) -> None:
    """
    Delete destination.

    Args:
        destination_id: Destination identifier
        service: Destination service instance
    """
    service.delete_destination(destination_id)
