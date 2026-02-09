"""
Configuration API endpoints.

Provides REST API for managing application configuration settings.
"""

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.core.logging import get_logger
from app.domain.schemas.configuration import (
    ConfigurationResponse,
    ConfigurationUpdate,
    WALThresholds,
)
from app.domain.services.configuration import ConfigurationService

logger = get_logger(__name__)
router = APIRouter(prefix="/configuration", tags=["Configuration"])


@router.get(
    "/wal-thresholds",
    response_model=WALThresholds,
    status_code=status.HTTP_200_OK,
    summary="Get WAL monitoring thresholds",
    description="Retrieve current WAL monitoring threshold configuration",
)
async def get_wal_thresholds(
    db: Session = Depends(get_db),
):
    """
    Get WAL monitoring threshold configuration.

    Returns:
        WAL thresholds including warning, error levels and webhook URL
    """
    try:
        service = ConfigurationService(db)
        thresholds = service.get_wal_thresholds()
        
        # Convert bytes back to MB for API response
        return WALThresholds(
            warning=thresholds.warning // (1024 * 1024),
            error=thresholds.error // (1024 * 1024),
            webhook_url=thresholds.webhook_url
        )
    except Exception as e:
        logger.error("Failed to get WAL thresholds", extra={"error": str(e)})
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve WAL threshold configuration",
        )


@router.put(
    "/wal-thresholds",
    response_model=WALThresholds,
    status_code=status.HTTP_200_OK,
    summary="Update WAL monitoring thresholds",
    description="Update WAL monitoring threshold configuration",
)
async def update_wal_thresholds(
    thresholds: WALThresholds,
    db: Session = Depends(get_db),
):
    """
    Update WAL monitoring threshold configuration.

    Args:
        thresholds: New threshold values (in MB)
        db: Database session

    Returns:
        Updated WAL thresholds
    """
    try:
        service = ConfigurationService(db)
        
        # Update each configuration value
        service.set_value("WAL_MONITORING_THRESHOLD_WARNING", str(thresholds.warning))
        service.set_value("WAL_MONITORING_THRESHOLD_ERROR", str(thresholds.error))
        service.set_value("ALERT_NOTIFICATION_WEBHOOK_URL", thresholds.webhook_url)
        service.set_value("NOTIFICATION_ITERATION_DEFAULT", str(thresholds.notification_iteration))
        
        logger.info(
            "WAL thresholds updated",
            extra={
                "warning": thresholds.warning,
                "error": thresholds.error,
                "webhook_url": thresholds.webhook_url,
                "notification_iteration": thresholds.notification_iteration
            }
        )
        
        return thresholds
    except Exception as e:
        logger.error("Failed to update WAL thresholds", extra={"error": str(e)})
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update WAL threshold configuration",
        )
