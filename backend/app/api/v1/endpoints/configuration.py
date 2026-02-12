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
    TestNotificationRequest,
    BatchConfiguration,
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
            webhook_url=thresholds.webhook_url,
            notification_iteration=thresholds.notification_iteration
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


@router.post(
    "/wal-thresholds/test",
    status_code=status.HTTP_200_OK,
    summary="Test notification webhook",
    description="Send a test notification to the configured webhook URL or a provided one",
)
async def test_notification_webhook(
    request: TestNotificationRequest = None,
    db: Session = Depends(get_db),
):
    """
    Trigger a test notification.
    
    Args:
        request: Optional body containing webhook_url
        db: Database session
        
    Returns:
        Success message or error
    """
    try:
        from app.domain.services.notification_service import NotificationService
        
        webhook_url = request.webhook_url if request else None
        
        service = NotificationService(db)
        success = await service.send_test_notification(webhook_url)
        
        if success:
            return {"message": "Test notification sent successfully"}
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to send test notification. Check logs for details.",
            )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e),
        )
    except Exception as e:
        logger.error("Failed to send test notification", extra={"error": str(e)})
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to test notification configuration",
        )


@router.get(
    "/batch",
    response_model=BatchConfiguration,
    status_code=status.HTTP_200_OK,
    summary="Get batch configuration",
    description="Retrieve current CDC batch processing configuration",
)
async def get_batch_configuration(
    db: Session = Depends(get_db),
):
    """
    Get batch processing configuration.

    Returns:
        Batch configuration with max_batch_size and max_queue_size
    """
    try:
        service = ConfigurationService(db)
        config = service.get_batch_configuration()
        return config
    except Exception as e:
        logger.error("Failed to get batch configuration", extra={"error": str(e)})
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve batch configuration",
        )


@router.put(
    "/batch",
    response_model=BatchConfiguration,
    status_code=status.HTTP_200_OK,
    summary="Update batch configuration",
    description="Update CDC batch processing configuration",
)
async def update_batch_configuration(
    config: BatchConfiguration,
    db: Session = Depends(get_db),
):
    """
    Update batch processing configuration.

    Args:
        config: New batch configuration values
        db: Database session

    Returns:
        Updated batch configuration
    """
    try:
        service = ConfigurationService(db)
        updated_config = service.update_batch_configuration(config)
        
        logger.info(
            "Batch configuration updated - all active pipelines marked for refresh",
            extra={
                "max_batch_size": config.max_batch_size,
                "max_queue_size": config.max_queue_size,
            }
        )
        
        return updated_config
    except Exception as e:
        logger.error("Failed to update batch configuration", extra={"error": str(e)})
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update batch configuration",
        )
