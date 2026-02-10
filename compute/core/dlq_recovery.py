"""
DLQ Recovery Worker - Background thread for replaying dead letter queue messages.

Continuously monitors DLQ queues and attempts to replay messages when destinations
become available. Runs in a separate thread per pipeline.
"""

import logging
import threading
import time
from typing import Optional

from destinations.base import BaseDestination, CDCRecord
from core.dlq_manager import DLQManager, DLQMessage
from core.models import (
    Pipeline,
    PipelineDestinationTableSync,
)
from core.exceptions import DestinationException

logger = logging.getLogger(__name__)


class DLQRecoveryWorker:
    """
    Background worker for recovering messages from DLQ.

    Monitors all DLQ queues for a pipeline and attempts to replay messages
    when destinations are healthy and reachable.
    """

    def __init__(
        self,
        pipeline: Pipeline,
        destinations: dict[int, BaseDestination],
        dlq_manager: DLQManager,
        check_interval: int = 30,
        batch_size: int = 100,
    ):
        """
        Initialize DLQ recovery worker.

        Args:
            pipeline: Pipeline configuration
            destinations: Dict of destination_id -> BaseDestination
            dlq_manager: DLQ manager instance
            check_interval: Seconds between recovery attempts
            batch_size: Number of messages to process per batch
        """
        self._pipeline = pipeline
        self._destinations = destinations
        self._dlq_manager = dlq_manager
        self._check_interval = check_interval
        self._batch_size = batch_size

        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._logger = logging.getLogger(f"{__name__}.Pipeline_{pipeline.id}")

        # Track destination health: destination_id -> last_check_success
        self._destination_health: dict[int, bool] = {}
        self._health_check_lock = threading.Lock()

    def start(self) -> None:
        """Start the recovery worker thread."""
        if self._running:
            self._logger.warning("DLQ recovery worker already running")
            return

        self._running = True
        self._thread = threading.Thread(
            target=self._run_recovery_loop,
            name=f"DLQRecovery-Pipeline{self._pipeline.id}",
            daemon=True,
        )
        self._thread.start()
        self._logger.info(
            f"Started DLQ recovery worker for pipeline {self._pipeline.name} "
            f"(check_interval={self._check_interval}s, batch_size={self._batch_size})"
        )

    def stop(self) -> None:
        """Stop the recovery worker thread."""
        if not self._running:
            return

        self._logger.info(
            f"Stopping DLQ recovery worker for pipeline {self._pipeline.name}"
        )
        self._running = False

        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=10)
            if self._thread.is_alive():
                self._logger.warning("DLQ recovery worker did not stop gracefully")

    def _run_recovery_loop(self) -> None:
        """Main recovery loop - runs continuously until stopped."""
        self._logger.info("DLQ recovery loop started")

        while self._running:
            try:
                # Get all queue identifiers from DLQ
                queues = self._dlq_manager.list_queues()

                if queues:
                    self._logger.debug(f"Found {len(queues)} DLQ queues to process")

                    for source_id, table_name, destination_id in queues:
                        if not self._running:
                            break

                        # Process this queue
                        self._process_queue(source_id, table_name, destination_id)

                # Sleep before next check
                time.sleep(self._check_interval)

            except Exception as e:
                self._logger.error(f"Error in DLQ recovery loop: {e}", exc_info=True)
                time.sleep(self._check_interval)

        self._logger.info("DLQ recovery loop stopped")

    def _process_queue(
        self,
        source_id: int,
        table_name: str,
        destination_id: int,
    ) -> None:
        """
        Process a specific DLQ queue.

        Args:
            source_id: Source database ID
            table_name: Table name
            destination_id: Destination database ID
        """
        try:
            # Check if destination is healthy before attempting recovery
            if not self._check_destination_health(destination_id):
                self._logger.debug(
                    f"Destination {destination_id} unhealthy, skipping recovery for "
                    f"source_{source_id}/table_{table_name}"
                )
                return

            # Check if queue has messages
            if not self._dlq_manager.has_messages(
                source_id, table_name, destination_id
            ):
                return

            # Dequeue batch of messages
            messages = self._dlq_manager.dequeue_batch(
                source_id,
                table_name,
                destination_id,
                max_messages=self._batch_size,
            )

            if not messages:
                return

            self._logger.info(
                f"Processing {len(messages)} DLQ messages for "
                f"source_{source_id}/table_{table_name}/dest_{destination_id}"
            )

            # Attempt to replay messages
            self._replay_messages(messages, destination_id)

        except Exception as e:
            self._logger.error(
                f"Error processing DLQ queue source_{source_id}/table_{table_name}/dest_{destination_id}: {e}",
                exc_info=True,
            )

    def _check_destination_health(self, destination_id: int) -> bool:
        """
        Check if destination is healthy and reachable.

        Args:
            destination_id: Destination ID to check

        Returns:
            True if destination is healthy
        """
        destination = self._destinations.get(destination_id)
        if not destination:
            self._logger.warning(f"Destination {destination_id} not found in pipeline")
            return False

        try:
            # Test connection
            is_healthy = destination.test_connection()

            with self._health_check_lock:
                prev_health = self._destination_health.get(destination_id)
                self._destination_health[destination_id] = is_healthy

                # Log state changes
                if prev_health is False and is_healthy:
                    self._logger.info(
                        f"✓ Destination {destination.name} (ID={destination_id}) is now HEALTHY - "
                        f"will attempt DLQ recovery"
                    )
                elif prev_health is True and not is_healthy:
                    self._logger.warning(
                        f"✗ Destination {destination.name} (ID={destination_id}) is now UNHEALTHY"
                    )

            return is_healthy

        except Exception as e:
            self._logger.debug(
                f"Health check failed for destination {destination_id}: {e}"
            )
            with self._health_check_lock:
                self._destination_health[destination_id] = False
            return False

    def _replay_messages(
        self,
        messages: list[DLQMessage],
        destination_id: int,
    ) -> None:
        """
        Attempt to replay DLQ messages to destination.

        Args:
            messages: List of DLQ messages to replay
            destination_id: Destination ID
        """
        destination = self._destinations.get(destination_id)
        if not destination:
            self._logger.error(f"Destination {destination_id} not found")
            # Re-enqueue messages
            self._re_enqueue_messages(messages)
            return

        # Group messages by table for batch processing
        messages_by_table: dict[str, list[DLQMessage]] = {}
        for msg in messages:
            table_key = msg.table_name
            if table_key not in messages_by_table:
                messages_by_table[table_key] = []
            messages_by_table[table_key].append(msg)

        # Process each table's messages
        for table_name, table_messages in messages_by_table.items():
            self._replay_table_messages(table_messages, destination)

    def _replay_table_messages(
        self,
        messages: list[DLQMessage],
        destination: BaseDestination,
    ) -> None:
        """
        Replay messages for a specific table.

        Args:
            messages: List of DLQ messages for same table
            destination: Destination to write to
        """
        if not messages:
            return

        # All messages should have same routing info
        first_msg = messages[0]
        table_name = first_msg.table_name
        table_name_target = first_msg.table_name_target

        # Reconstruct table_sync from stored config
        table_sync = self._create_table_sync_from_config(first_msg.table_sync_config)

        # Extract CDC records
        cdc_records = [msg.cdc_record for msg in messages]

        try:
            # Ensure destination is initialized before writing
            if not destination._is_initialized:
                self._logger.debug(
                    f"Initializing destination {destination.name} before replay"
                )
                destination.initialize()

            # Attempt to write batch
            written = destination.write_batch(cdc_records, table_sync)

            self._logger.info(
                f"✓ Successfully replayed {written}/{len(cdc_records)} DLQ messages to "
                f"{destination.name} for table {table_name}"
            )

            # Success! Messages are already dequeued, no need to re-enqueue

        except DestinationException as e:
            # Destination error - re-enqueue with incremented retry count
            self._logger.warning(
                f"Failed to replay DLQ messages to {destination.name} for table {table_name}: {e}"
            )
            self._re_enqueue_messages(messages, increment_retry=True)

        except Exception as e:
            # Unexpected error - re-enqueue
            self._logger.error(
                f"Unexpected error replaying DLQ messages to {destination.name} for table {table_name}: {e}",
                exc_info=True,
            )
            self._re_enqueue_messages(messages, increment_retry=True)

    def _re_enqueue_messages(
        self,
        messages: list[DLQMessage],
        increment_retry: bool = False,
    ) -> None:
        """
        Re-enqueue messages back to DLQ after failed replay.

        Args:
            messages: Messages to re-enqueue
            increment_retry: Whether to increment retry counter
        """
        for msg in messages:
            if increment_retry:
                msg.retry_count += 1

            # Re-enqueue by pushing back to queue
            queue = self._dlq_manager._get_or_create_queue(
                msg.source_id,
                msg.table_name,
                msg.destination_id,
            )
            queue.push([msg.to_bytes()], no_gil=True)

    def _create_table_sync_from_config(
        self,
        config: dict,
    ) -> PipelineDestinationTableSync:
        """
        Create PipelineDestinationTableSync from stored config.

        This is a lightweight object just for passing to write_batch.
        Not a full ORM object.
        """

        # Create a simple object that has the required attributes
        class TableSyncStub:
            def __init__(self, config_dict):
                self.id = config_dict.get("id")
                self.table_name = config_dict.get("table_name")
                self.table_name_target = config_dict.get("table_name_target")
                self.filter_sql = config_dict.get("filter_sql")
                self.custom_sql = config_dict.get("custom_sql")

        return TableSyncStub(config)

    def get_stats(self) -> dict:
        """
        Get recovery worker statistics.

        Returns:
            Dict with statistics
        """
        queues = self._dlq_manager.list_queues()

        with self._health_check_lock:
            health_stats = dict(self._destination_health)

        return {
            "running": self._running,
            "pipeline_id": self._pipeline.id,
            "pipeline_name": self._pipeline.name,
            "check_interval": self._check_interval,
            "batch_size": self._batch_size,
            "total_queues": len(queues),
            "destination_health": health_stats,
        }
