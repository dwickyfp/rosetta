"""
WAL Monitor API endpoints.

Provides REST API for managing real-time WAL monitoring status.
Implements upsert pattern: 1 source = 1 monitor record.
"""

from typing import List

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.core.exceptions import EntityNotFoundError
from app.core.logging import get_logger
from app.domain.schemas.wal_monitor import (
    WALMonitorCreate,
    WALMonitorListResponse,
    WALMonitorResponse,
    WALMonitorStatusUpdate,
)
from app.domain.services.wal_monitor_service import WALMonitorService

logger = get_logger(__name__)
router = APIRouter(prefix="/wal-monitor", tags=["WAL Monitor"])


@router.post(
    "/sources/{source_id}",
    response_model=WALMonitorResponse,
    status_code=status.HTTP_200_OK,
    summary="Upsert WAL monitor record",
    description=(
        "Create or update WAL monitor record for a source. "
        "If record exists, it will be updated. Otherwise, created. "
        "Ensures 1 source has exactly 1 monitor record."
    ),
)
async def upsert_wal_monitor(
    source_id: int,
    data: WALMonitorCreate,
    db: Session = Depends(get_db),
):
    """
    Create or update WAL monitor record for a source.

    This endpoint implements upsert logic:
    - If monitor record exists for source_id -> UPDATE
    - If monitor record doesn't exist -> INSERT

    Args:
        source_id: Source identifier
        data: WAL monitor data
        db: Database session

    Returns:
        Created or updated WAL monitor record

    Raises:
        404: Source not found
        500: Database error
    """
    try:
        # Ensure source_id in path matches body (if provided)
        data.source_id = source_id

        service = WALMonitorService(db)
        monitor = service.upsert_monitor(source_id, data)

        logger.info(
            "WAL monitor upserted via API",
            extra={"source_id": source_id, "monitor_id": monitor.id},
        )

        return monitor

    except EntityNotFoundError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e),
        )
    except Exception as e:
        logger.error("Failed to upsert WAL monitor", extra={"error": str(e)})
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to upsert WAL monitor record",
        )


@router.get(
    "/sources/{source_id}",
    response_model=WALMonitorResponse,
    status_code=status.HTTP_200_OK,
    summary="Get WAL monitor by source",
    description="Retrieve WAL monitor record for a specific source",
)
async def get_wal_monitor_by_source(
    source_id: int,
    db: Session = Depends(get_db),
):
    """
    Get WAL monitor record for a source.

    Args:
        source_id: Source identifier
        db: Database session

    Returns:
        WAL monitor record

    Raises:
        404: Monitor record not found
        500: Database error
    """
    try:
        service = WALMonitorService(db)
        monitor = service.get_by_source(source_id)

        if not monitor:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"WAL monitor not found for source_id={source_id}",
            )

        return monitor

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to get WAL monitor", extra={"error": str(e)})
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve WAL monitor record",
        )


@router.get(
    "/",
    response_model=WALMonitorListResponse,
    status_code=status.HTTP_200_OK,
    summary="List all WAL monitors",
    description="Retrieve all WAL monitor records",
)
async def list_wal_monitors(
    db: Session = Depends(get_db),
):
    """
    Get all WAL monitor records.

    Args:
        db: Database session

    Returns:
        List of all WAL monitor records

    Raises:
        500: Database error
    """
    try:
        service = WALMonitorService(db)
        monitors = service.list_monitors()

        return WALMonitorListResponse(
            monitors=monitors,
            total=len(monitors),
        )

    except Exception as e:
        logger.error("Failed to list WAL monitors", extra={"error": str(e)})
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve WAL monitor records",
        )


@router.patch(
    "/sources/{source_id}/status",
    response_model=WALMonitorResponse,
    status_code=status.HTTP_200_OK,
    summary="Update WAL monitor status",
    description="Update status field of WAL monitor record",
)
async def update_wal_monitor_status(
    source_id: int,
    data: WALMonitorStatusUpdate,
    db: Session = Depends(get_db),
):
    """
    Update WAL monitor status.

    Args:
        source_id: Source identifier
        data: Status update data
        db: Database session

    Returns:
        Updated WAL monitor record

    Raises:
        404: Monitor record not found
        500: Database error
    """
    try:
        service = WALMonitorService(db)
        monitor = service.update_status(source_id, data)

        logger.info(
            "WAL monitor status updated via API",
            extra={"source_id": source_id, "status": data.status},
        )

        return monitor

    except EntityNotFoundError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e),
        )
    except Exception as e:
        logger.error("Failed to update WAL monitor status", extra={"error": str(e)})
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update WAL monitor status",
        )


@router.delete(
    "/sources/{source_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete WAL monitor record",
    description="Delete WAL monitor record for a source",
)
async def delete_wal_monitor(
    source_id: int,
    db: Session = Depends(get_db),
):
    """
    Delete WAL monitor record for a source.

    Args:
        source_id: Source identifier
        db: Database session

    Returns:
        No content on success

    Raises:
        404: Monitor record not found
        500: Database error
    """
    try:
        service = WALMonitorService(db)
        service.delete_by_source(source_id)

        logger.info("WAL monitor deleted via API", extra={"source_id": source_id})

    except EntityNotFoundError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e),
        )
    except Exception as e:
        logger.error("Failed to delete WAL monitor", extra={"error": str(e)})
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to delete WAL monitor record",
        )
