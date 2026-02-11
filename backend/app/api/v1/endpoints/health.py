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

import httpx
import asyncio
from app.core.logging import get_logger

logger = get_logger(__name__)

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
    db_healthy = await asyncio.to_thread(check_database_health)

    # Check Redis health
    redis_healthy = False
    try:
        from app.infrastructure.redis import RedisClient
        redis_client = RedisClient.get_instance()
        redis_healthy = await redis_client.ping() if asyncio.iscoroutinefunction(redis_client.ping) else redis_client.ping()
    except Exception:
        redis_healthy = False

    # Check Compute Node health
    compute_healthy = False
    url = f"{settings.compute_node_url}/health"
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.get(url)
            compute_healthy = response.status_code == 200 and response.json().get("status") == "healthy"
            if not compute_healthy:
                logger.warning(f"Compute health check returned {response.status_code}: {response.text}")
    except Exception as e:
        logger.warning(f"Compute health check failed: {e}")
        compute_healthy = False

    # Determine overall status
    # We consider healthy if DB and Redis are up. Compute is optional for API but tracked.
    overall_status = "healthy" if db_healthy and redis_healthy else "unhealthy"

    return HealthResponse(
        status=overall_status,
        version=__version__,
        timestamp=datetime.now(timezone(timedelta(hours=7))),
        checks={
            "database": db_healthy, 
            "redis": redis_healthy,
            "wal_monitor": settings.wal_monitor_enabled,
            "compute": compute_healthy
        },
    )
