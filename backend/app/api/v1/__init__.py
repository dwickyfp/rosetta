"""
API v1 initialization module.

Exports the main API router.
"""

from fastapi import APIRouter

from app.api.v1.endpoints import (
    destinations,
    health,
    pipelines,
    sources,
    wal_metrics,
    wal_monitor,
)

# Create v1 router
api_router = APIRouter()

# Include all endpoint routers
api_router.include_router(health.router, prefix="/health", tags=["health"])

api_router.include_router(sources.router, prefix="/sources", tags=["sources"])

api_router.include_router(
    destinations.router, prefix="/destinations", tags=["destinations"]
)

api_router.include_router(pipelines.router, prefix="/pipelines", tags=["pipelines"])

api_router.include_router(wal_metrics.router, prefix="/metrics", tags=["metrics"])

api_router.include_router(wal_monitor.router, tags=["wal-monitor"])
