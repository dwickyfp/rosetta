"""
Health check endpoints.

Provides application health and status information.
"""

from datetime import datetime, timezone, timedelta

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
    # Check database health
    db_healthy = check_database_health()

    # Check Redis health
    redis_healthy = False
    try:
        from app.infrastructure.redis import RedisClient
        redis_client = RedisClient.get_instance()
        redis_healthy = redis_client.ping()
    except Exception:
        redis_healthy = False

    # Determine overall status
    overall_status = "healthy" if db_healthy and redis_healthy else "unhealthy"

    return HealthResponse(
        status=overall_status,
        version=__version__,
        timestamp=datetime.now(timezone(timedelta(hours=7))),
        checks={
            "database": db_healthy, 
            "redis": redis_healthy,
            "wal_monitor": settings.wal_monitor_enabled
        },
    )
