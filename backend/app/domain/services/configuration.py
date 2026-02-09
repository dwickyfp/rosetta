"""
Configuration Service.

Manages application configuration settings.
"""

from sqlalchemy.orm import Session
from app.domain.repositories.configuration_repo import ConfigurationRepository
from app.domain.schemas.configuration import WALThresholds


class ConfigurationService:
    """Service for managing configuration settings."""
    
    def __init__(self, db: Session):
        """Initialize service."""
        self.repo = ConfigurationRepository(db)
    
    def get_wal_thresholds(self) -> WALThresholds:
        """
        Get WAL monitoring thresholds from configuration.
        
        Note: Thresholds in database are stored in MB and converted to bytes here.
        
        Returns:
            WAL thresholds configuration with values in bytes
        """
        # Get values in MB from database
        warning_mb = int(self.repo.get_value('WAL_MONITORING_THRESHOLD_WARNING', '3000'))
        error_mb = int(self.repo.get_value('WAL_MONITORING_THRESHOLD_ERROR', '6000'))
        webhook_url = self.repo.get_value('ALERT_NOTIFICATION_WEBHOOK_URL', '')
        notification_iteration = int(self.repo.get_value('NOTIFICATION_ITERATION_DEFAULT', '3'))
        
        # Convert MB to bytes (1 MB = 1024 * 1024 bytes)
        warning_bytes = warning_mb * 1024 * 1024
        error_bytes = error_mb * 1024 * 1024
        
        return WALThresholds(
            warning=warning_bytes,
            error=error_bytes,
            webhook_url=webhook_url,
            notification_iteration=notification_iteration
        )
    
    def get_value(self, key: str, default: str = "") -> str:
        """
        Get configuration value by key.
        
        Args:
            key: Configuration key
            default: Default value if not found
            
        Returns:
            Configuration value
        """
        return self.repo.get_value(key, default)
    
    def set_value(self, key: str, value: str):
        """
        Set configuration value.
        
        Args:
            key: Configuration key
            value: Configuration value
        """
        return self.repo.set_value(key, value)
