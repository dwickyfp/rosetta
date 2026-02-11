from fastapi import APIRouter

from app.api.v1.endpoints import (
    destinations,
    health,
    pipelines,
    sources,
    wal_metrics,
    wal_monitor,
    system_metrics,
    credits,
    wal_monitor,
    system_metrics,
    credits,
    configuration,
    dashboard,
    table_sync,
    job_metrics,
    notification_logs,
    backfill,
)

# Create v1 router
api_router = APIRouter()

# Include all endpoint routers
api_router.include_router(health.router, prefix="/health", tags=["health"])

api_router.include_router(sources.router, prefix="/sources", tags=["sources"])

api_router.include_router(
    destinations.router, prefix="/destinations", tags=["destinations"]
)

api_router.include_router(credits.router, prefix="/destinations", tags=["credits"])

api_router.include_router(pipelines.router, prefix="/pipelines", tags=["pipelines"])

api_router.include_router(table_sync.router, prefix="/pipelines", tags=["table-sync"])

api_router.include_router(wal_metrics.router, prefix="/metrics", tags=["metrics"])

api_router.include_router(wal_monitor.router, tags=["wal-monitor"])

api_router.include_router(system_metrics.router, tags=["system-metrics"])

api_router.include_router(configuration.router, tags=["configuration"])

api_router.include_router(dashboard.router, prefix="/dashboard", tags=["dashboard"])

api_router.include_router(
    job_metrics.router, prefix="/job-metrics", tags=["job-metrics"]
)

api_router.include_router(notification_logs.router, tags=["notification-logs"])

api_router.include_router(backfill.router, tags=["backfill"])
