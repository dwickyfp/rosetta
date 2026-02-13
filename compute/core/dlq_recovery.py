"""
DLQ Recovery Worker - Background thread for replaying dead letter queue messages.

Continuously monitors DLQ queues and attempts to replay messages when destinations
become available. Uses Redis Streams consumer groups for at-least-once delivery.
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
from core.repository import PipelineDestinationRepository, TableSyncRepository

logger = logging.getLogger(__name__)


class DLQRecoveryWorker:
    """
    Background worker for recovering messages from DLQ.

    Monitors all DLQ queues for a pipeline and attempts to replay messages
    when destinations are healthy and reachable. Uses Redis Streams consumer
    groups — unacknowledged messages are automatically retried on next cycle.
    """

    def __init__(
        self,
        pipeline: Pipeline,
        destinations: dict[int, BaseDestination],
        dlq_manager: DLQManager,
        check_interval: int = 30,
        batch_size: int = 100,
        max_retry_count: int = 10,
        max_age_days: int = 7,
        consumer_name: str = "worker-1",
    ):
        """
        Initialize DLQ recovery worker.

        Args:
            pipeline: Pipeline configuration
            destinations: Dict of destination_id -> BaseDestination
            dlq_manager: DLQ manager instance
            check_interval: Seconds between recovery attempts
            batch_size: Number of messages to process per batch
            max_retry_count: Max retries before discarding a message
            max_age_days: Max age in days before purging a message
            consumer_name: Consumer name within the consumer group
        """
        self._pipeline = pipeline
        self._destinations = destinations
        self._dlq_manager = dlq_manager
        self._check_interval = check_interval
        self._batch_size = batch_size
        self._max_retry_count = max_retry_count
        self._max_age_days = max_age_days
        self._consumer_name = consumer_name

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

        # Track iterations for periodic cleanup
        iteration_count = 0
        cleanup_interval = (
            10  # Run purge every 10 iterations (10 * check_interval seconds)
        )

        while self._running:
            try:
                iteration_count += 1

                # Get all queue identifiers from DLQ
                queues = self._dlq_manager.list_queues()

                if queues:
                    self._logger.debug(f"Found {len(queues)} DLQ queues to process")

                    for source_id, table_name, destination_id in queues:
                        if not self._running:
                            break

                        # First, claim any stale messages from dead consumers
                        self._claim_and_process_stale(
                            source_id, table_name, destination_id
                        )

                        # Then process new messages
                        self._process_queue(source_id, table_name, destination_id)

                        # Periodic cleanup: purge old messages
                        if iteration_count % cleanup_interval == 0:
                            self._dlq_manager.purge_old_messages(
                                source_id,
                                table_name,
                                destination_id,
                                max_retry_count=self._max_retry_count,
                                max_age_days=self._max_age_days,
                            )

                # Sleep before next check
                time.sleep(self._check_interval)

            except Exception as e:
                self._logger.error(f"Error in DLQ recovery loop: {e}", exc_info=True)
                time.sleep(self._check_interval)

        self._logger.info("DLQ recovery loop stopped")

    def _claim_and_process_stale(
        self,
        source_id: int,
        table_name: str,
        destination_id: int,
    ) -> None:
        """
        Claim stale pending messages from dead consumers and process them.

        Uses XAUTOCLAIM to take over messages that have been pending for too long
        (e.g., from a crashed worker).
        """
        try:
            stale_messages = self._dlq_manager.claim_stale_messages(
                source_id=source_id,
                table_name=table_name,
                destination_id=destination_id,
                min_idle_ms=self._check_interval * 3 * 1000,  # 3x check interval
                max_messages=self._batch_size,
                consumer_name=self._consumer_name,
            )

            if stale_messages:
                self._logger.info(
                    f"Claimed {len(stale_messages)} stale messages for "
                    f"s{source_id}:t{table_name}:d{destination_id}"
                )
                self._replay_messages_with_ids(
                    stale_messages, source_id, table_name, destination_id
                )

        except Exception as e:
            self._logger.error(
                f"Error claiming stale messages: "
                f"s{source_id}:t{table_name}:d{destination_id}: {e}",
                exc_info=True,
            )

    def _process_queue(
        self,
        source_id: int,
        table_name: str,
        destination_id: int,
    ) -> None:
        """
        Process a specific DLQ queue by reading new messages.

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
                    f"s{source_id}:t{table_name}"
                )
                return

            # Check if queue has messages (non-destructive XLEN check)
            if not self._dlq_manager.has_messages(
                source_id, table_name, destination_id
            ):
                return

            # Dequeue batch of messages via XREADGROUP
            messages_with_ids = self._dlq_manager.dequeue_batch(
                source_id,
                table_name,
                destination_id,
                max_messages=self._batch_size,
                consumer_name=self._consumer_name,
            )

            if not messages_with_ids:
                return

            self._logger.info(
                f"Processing {len(messages_with_ids)} DLQ messages for "
                f"s{source_id}:t{table_name}:d{destination_id}"
            )

            # Attempt to replay messages
            self._replay_messages_with_ids(
                messages_with_ids, source_id, table_name, destination_id
            )

        except Exception as e:
            self._logger.error(
                f"Error processing DLQ queue s{source_id}:t{table_name}:d{destination_id}: {e}",
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

    def _replay_messages_with_ids(
        self,
        messages_with_ids: list[tuple[str, DLQMessage]],
        source_id: int,
        table_name: str,
        destination_id: int,
    ) -> None:
        """
        Attempt to replay DLQ messages to destination.

        On success: ACK + DEL the messages (they're done).
        On failure: Update retry count. If max retries exceeded, discard.

        Args:
            messages_with_ids: List of (message_id, DLQMessage) tuples
            source_id: Source database ID
            table_name: Table name
            destination_id: Destination database ID
        """
        destination = self._destinations.get(destination_id)
        if not destination:
            self._logger.error(f"Destination {destination_id} not found")
            # Leave messages unacked — they'll be retried on next cycle
            return

        # Group messages by table for batch processing
        messages_by_table: dict[str, list[tuple[str, DLQMessage]]] = {}
        for msg_id, msg in messages_with_ids:
            table_key = msg.table_name
            if table_key not in messages_by_table:
                messages_by_table[table_key] = []
            messages_by_table[table_key].append((msg_id, msg))

        # Process each table's messages
        for tbl_name, table_messages in messages_by_table.items():
            self._replay_table_messages(
                table_messages, destination, source_id, tbl_name, destination_id
            )

    def _replay_table_messages(
        self,
        messages_with_ids: list[tuple[str, DLQMessage]],
        destination: BaseDestination,
        source_id: int,
        table_name: str,
        destination_id: int,
    ) -> None:
        """
        Replay messages for a specific table.

        Args:
            messages_with_ids: List of (message_id, DLQMessage) tuples
            destination: Destination to write to
            source_id: Source database ID
            table_name: Table name
            destination_id: Destination database ID
        """
        if not messages_with_ids:
            return

        # All messages should have same routing info
        first_msg = messages_with_ids[0][1]
        table_name_target = first_msg.table_name_target

        # Reconstruct table_sync from stored config
        table_sync = self._create_table_sync_from_config(first_msg.table_sync_config)

        # Extract CDC records and message IDs
        cdc_records = [msg.cdc_record for _, msg in messages_with_ids]
        message_ids = [msg_id for msg_id, _ in messages_with_ids]

        try:
            # Ensure destination is initialized before writing
            if not destination._is_initialized:
                self._logger.debug(
                    f"Initializing destination {destination.name} before replay"
                )
                destination.initialize()
            else:
                self._logger.debug(
                    f"Checking connection health for destination {destination.name}"
                )
                destination.initialize(force_reconnect=False)

            # Attempt to write batch
            written = destination.write_batch(cdc_records, table_sync)

            self._logger.info(
                f"✓ Successfully replayed {written}/{len(cdc_records)} DLQ messages to "
                f"{destination.name} for table {table_name}"
            )

            # Success! Clear error flags (same as CDC behavior)
            # Get pipeline_destination_id and table_sync_id from table_sync_config
            pipeline_destination_id = first_msg.table_sync_config.get(
                "pipeline_destination_id"
            )
            table_sync_id = first_msg.table_sync_config.get("id")

            # Clear error flag on pipeline_destination
            if pipeline_destination_id:
                PipelineDestinationRepository.update_error(
                    pipeline_destination_id, False
                )
                self._logger.debug(
                    f"Cleared error flag for pipeline_destination {pipeline_destination_id}"
                )

            # Clear error flag on table_sync
            if table_sync_id:
                TableSyncRepository.update_error(table_sync_id, False)
                self._logger.debug(f"Cleared error flag for table_sync {table_sync_id}")

            # Acknowledge all messages (ACK + DEL)
            self._dlq_manager.acknowledge(
                source_id, table_name, destination_id, message_ids
            )

            # Check if queue is now empty and clean up
            if not self._dlq_manager.has_messages(
                source_id, table_name, destination_id
            ):
                self._logger.info(
                    f"Queue empty after replay, cleaning up: "
                    f"s{source_id}:t{table_name}:d{destination_id}"
                )
                self._dlq_manager.delete_queue(source_id, table_name, destination_id)

        except (DestinationException, Exception) as e:
            is_dest_error = isinstance(e, DestinationException)
            log_method = self._logger.warning if is_dest_error else self._logger.error

            log_method(
                f"Failed to replay DLQ messages to {destination.name} for table {table_name}: {e}",
                exc_info=not is_dest_error,
            )

            # Update retry count for each message
            self._handle_retry(messages_with_ids, source_id, table_name, destination_id)

    def _handle_retry(
        self,
        messages_with_ids: list[tuple[str, DLQMessage]],
        source_id: int,
        table_name: str,
        destination_id: int,
    ) -> None:
        """
        Handle retry logic for failed messages.

        Increments retry_count. If max retries exceeded, acknowledges and discards
        the message. Otherwise, creates a new entry with updated retry count and
        removes the old one.

        Args:
            messages_with_ids: List of (message_id, DLQMessage) tuples
            source_id: Source database ID
            table_name: Table name
            destination_id: Destination database ID
        """
        ids_to_discard = []
        for msg_id, msg in messages_with_ids:
            msg.retry_count += 1

            if msg.retry_count >= self._max_retry_count:
                # Max retries exceeded — discard message
                self._logger.warning(
                    f"Discarding DLQ message after {msg.retry_count} retries: "
                    f"s{source_id}:t{table_name}:d{destination_id} "
                    f"operation={msg.cdc_record.operation}, key={msg.cdc_record.key}"
                )
                ids_to_discard.append(msg_id)
            else:
                # Replace with updated retry count (XADD new + XACK/XDEL old)
                self._dlq_manager.update_message_retry(
                    source_id, table_name, destination_id, msg_id, msg
                )

        # Discard messages that exceeded max retries
        if ids_to_discard:
            self._dlq_manager.acknowledge(
                source_id, table_name, destination_id, ids_to_discard
            )

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
                self.pipeline_destination_id = config_dict.get(
                    "pipeline_destination_id"
                )
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

        # Gather queue sizes
        queue_stats = []
        for source_id, table_name, dest_id in queues:
            size = self._dlq_manager.get_queue_size(source_id, table_name, dest_id)
            queue_stats.append(
                {
                    "source_id": source_id,
                    "table_name": table_name,
                    "destination_id": dest_id,
                    "size": size,
                }
            )

        with self._health_check_lock:
            health_stats = dict(self._destination_health)

        return {
            "running": self._running,
            "pipeline_id": self._pipeline.id,
            "pipeline_name": self._pipeline.name,
            "check_interval": self._check_interval,
            "batch_size": self._batch_size,
            "total_queues": len(queues),
            "queues": queue_stats,
            "destination_health": health_stats,
        }
