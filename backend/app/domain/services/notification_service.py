"""
Notification Service.

Handles sending notifications to external webhooks based on configuration.
"""

import httpx
import asyncio
from datetime import datetime, timezone, timedelta
from typing import List, Optional

from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.core.logging import get_logger
from app.domain.models.notification_log import NotificationLog
from app.domain.repositories.notification_log_repo import NotificationLogRepository
from app.domain.repositories.configuration_repo import ConfigurationRepository

logger = get_logger(__name__)


class NotificationService:
    """Service for managing and sending notifications."""

    def __init__(self, db: Session):
        """Initialize service."""
        self.db = db
        self.repo = NotificationLogRepository(db)
        self.config_repo = ConfigurationRepository(db)
        self.settings = get_settings()

    async def _send_webhook(self, url: str, payload: dict) -> bool:
        """
        Send payload to webhook URL.
        
        Args:
            url: Webhook URL
            payload: JSON payload
            
        Returns:
            True if successful, False otherwise
        """
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.post(url, json=payload)
                response.raise_for_status()
                logger.info(f"Notification sent successfully to {url}")
                return True
        except Exception as e:
            logger.error(f"Failed to send notification to {url}: {e}")
            return False

    async def process_pending_notifications(self) -> int:
        """
        Process pending notifications and send them to the webhook.
        
        Logic:
        1. Check if webhook URL is configured.
        2. Fetch notifications where:
           - iteration_check >= limit (default 3)
           - is_sent = False
           - is_deleted = False
           - is_read = False
        3. Send to webhook.
        4. Mark as sent.
        
        Returns:
            Number of notifications sent.
        """
        # 1. Check configuration
        webhook_url = self.config_repo.get_value("ALERT_NOTIFICATION_WEBHOOK_URL")
        if not webhook_url:
            logger.debug("No webhook URL configured, skipping notification sending")
            return 0
            
        iteration_limit_str = self.config_repo.get_value("NOTIFICATION_ITERATION_DEFAULT", "3")
        try:
            iteration_limit = int(iteration_limit_str)
        except ValueError:
            iteration_limit = 3

        # 2. Fetch pending notifications
        # We need a custom query here as repo might not have this specific filter
        # "iteration_check is equals with config NOTIFICATION_ITERATION_DEFAULT"
        # "is_sent is false", "is_deleted is false", "is_read is false"
        
        pending_notifications = (
            self.db.query(NotificationLog)
            .filter(
                NotificationLog.iteration_check >= iteration_limit,
                NotificationLog.is_sent == False,
                NotificationLog.is_deleted == False,
                NotificationLog.is_read == False
            )
            .all()
        )
        
        if not pending_notifications:
            return 0
            
        sent_count = 0
        now = datetime.now(timezone(timedelta(hours=7)))
        
        # 3. Process each
        for notification in pending_notifications:
            payload = {
                "key_notification": notification.key_notification,
                "title": notification.title,
                "message": notification.message,
                "type": notification.type,
                "timestamp": notification.created_at.isoformat() if notification.created_at else None
            }
            
            # Send (awaiting inside the loop, could be parallelized but sequential is safer for now)
            success = await self._send_webhook(webhook_url, payload)
            
            if success:
                # 4. Update status
                notification.is_sent = True
                notification.updated_at = now
                sent_count += 1
        
        if sent_count > 0:
            self.db.commit()
            
        return sent_count

    async def send_test_notification(self, webhook_url: Optional[str] = None) -> bool:
        """
        Send a test notification to the configured webhook.
        
        Args:
            webhook_url: Optional webhook URL to use. If not provided, uses configured URL.
        
        Returns:
            True if successful, False otherwise
        """
        if not webhook_url:
            webhook_url = self.config_repo.get_value("ALERT_NOTIFICATION_WEBHOOK_URL")
            
        if not webhook_url:
            raise ValueError("Webhook URL is not configured")
            
        payload = {
            "key_notification": "TEST_NOTIFICATION",
            "title": "Test Notification",
            "message": "This is a test notification from Rosetta ETL Platform.",
            "type": "TEST",
            "timestamp": datetime.now(timezone(timedelta(hours=7))).isoformat()
        }
        
        return await self._send_webhook(webhook_url, payload)
