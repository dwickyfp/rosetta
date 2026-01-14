"""
Health check endpoints.

Provides application health and status information.
"""

from datetime import datetime

from fastapi import APIRouter, Depends

from app import __version__
from app.core.config import get_settings
from app.core.database import check_database_health
from app.domain.schemas.common import HealthResponse

router = APIRouter()


@router.get(
    "",
    response_model=HealthResponse,
    summary="Health check",
    description="Check application health and dependency status",
)
async def health_check() -> HealthResponse:
    """
    Check application health.

    Returns health status including database connectivity.
    """
    settings = get_settings()

    # Check database health
    db_healthy = await check_database_health()

    # Determine overall status
    overall_status = "healthy" if db_healthy else "unhealthy"

    return HealthResponse(
        status=overall_status,
        version=__version__,
        timestamp=datetime.utcnow(),
        checks={"database": db_healthy, "wal_monitor": settings.wal_monitor_enabled},
    )
