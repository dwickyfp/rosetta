"""
Background task scheduler.

Manages scheduling and execution of background tasks like WAL monitoring.
"""

import asyncio
from typing import Optional

from apscheduler.schedulers.background import (
    BackgroundScheduler as APSBackgroundScheduler,
)
from apscheduler.triggers.interval import IntervalTrigger

from app.core.config import get_settings
from app.core.logging import get_logger
from app.domain.services.wal_monitor import WALMonitorService
from app.domain.services.replication_monitor import ReplicationMonitorService
from app.domain.services.schema_monitor import SchemaMonitorService

logger = get_logger(__name__)


class BackgroundScheduler:
    """
    Background task scheduler.

    Manages periodic execution of background tasks using APScheduler.
    """

    def __init__(self):
        """Initialize background scheduler."""
        self.settings = get_settings()
        self.scheduler: Optional[APSBackgroundScheduler] = None
        self.wal_monitor: Optional[WALMonitorService] = None
        self.replication_monitor: Optional[ReplicationMonitorService] = None
        self.schema_monitor: Optional[SchemaMonitorService] = None

    def _run_wal_monitor(self) -> None:
        """
        Synchronous wrapper for async WAL monitor task.
        
        This is needed because APScheduler BackgroundScheduler 
        expects synchronous functions.
        """
        try:
            asyncio.run(self.wal_monitor.monitor_all_sources())
        except Exception as e:
            logger.error("Error running WAL monitor task", extra={"error": str(e)})

    def _run_replication_monitor(self) -> None:
        """
        Synchronous wrapper for replication monitor task.
        """
        try:
            if self.replication_monitor:
                asyncio.run(self.replication_monitor.monitor_all_sources())
        except Exception as e:
            logger.error("Error running replication monitor task", extra={"error": str(e)})

    def _run_schema_monitor(self) -> None:
        """
        Synchronous wrapper for schema monitor task.
        """
        try:
            if self.schema_monitor:
                asyncio.run(self.schema_monitor.monitor_all_sources())
        except Exception as e:
            logger.error("Error running schema monitor task", extra={"error": str(e)})

    def start(self) -> None:
        """
        Start the background scheduler.

        Initializes and starts all scheduled tasks.
        """
        if not self.settings.background_task_enabled:
            logger.info("Background tasks are disabled in configuration")
            return

        logger.info("Starting background task scheduler")

        # Initialize scheduler
        self.scheduler = APSBackgroundScheduler(
            timezone=self.settings.scheduler_timezone
        )

        # Initialize WAL monitor
        if self.settings.wal_monitor_enabled:
            self.wal_monitor = WALMonitorService()

            # Schedule WAL monitoring task
            self.scheduler.add_job(
                self._run_wal_monitor,  # Use synchronous wrapper
                trigger=IntervalTrigger(
                    seconds=self.settings.wal_monitor_interval_seconds
                ),
                id="wal_monitor",
                name="PostgreSQL WAL Monitor",
                replace_existing=True,
                max_instances=1,  # Prevent concurrent executions
                coalesce=True,  # Combine missed executions
            )

            logger.info(
                "WAL monitoring scheduled",
                extra={"interval_seconds": self.settings.wal_monitor_interval_seconds},
            )

        # Initialize Replication monitor
        self.replication_monitor = ReplicationMonitorService()
        
        # Schedule Replication monitoring task
        self.scheduler.add_job(
            self._run_replication_monitor,
            trigger=IntervalTrigger(
                seconds=60  # Default to 60s
            ),
            id="replication_monitor",
            name="Replication Monitor",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
        )

        # Initialize Schema monitor
        self.schema_monitor = SchemaMonitorService()

        # Schedule Schema monitoring task
        self.scheduler.add_job(
            self._run_schema_monitor,
            trigger=IntervalTrigger(
                seconds=60  # Default to 60s
            ),
            id="schema_monitor",
            name="Schema Monitor",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
        )
        
        logger.info("Replication monitoring scheduled")

        # Start scheduler
        self.scheduler.start()
        logger.info("Background task scheduler started successfully")

    def stop(self) -> None:
        """
        Stop the background scheduler.

        Gracefully shuts down all scheduled tasks.
        """
        logger.info("Stopping background task scheduler")

        if self.scheduler:
            self.scheduler.shutdown(wait=True)
            self.scheduler = None

        if self.wal_monitor:
            self.wal_monitor.stop()
            self.wal_monitor = None

        if self.replication_monitor:
            self.replication_monitor.stop()
            self.replication_monitor = None
            
        if self.schema_monitor:
            self.schema_monitor.stop()
            self.schema_monitor = None

        logger.info("Background task scheduler stopped")

    def get_job_status(self) -> dict:
        """
        Get status of all scheduled jobs.

        Returns:
            Dictionary with job status information
        """
        if not self.scheduler:
            return {"status": "not_running", "jobs": []}

        jobs = []
        for job in self.scheduler.get_jobs():
            jobs.append(
                {
                    "id": job.id,
                    "name": job.name,
                    "next_run_time": (
                        str(job.next_run_time) if job.next_run_time else None
                    ),
                }
            )

        return {
            "status": "running" if self.scheduler.running else "stopped",
            "jobs": jobs,
        }
