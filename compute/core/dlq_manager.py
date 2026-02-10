"""
Dead Letter Queue (DLQ) Manager using RocksQ.

Provides persistent message queuing for CDC records that fail to reach destinations.
Messages are organized by source_id, table_name, and destination_id for granular control.
"""

import json
import logging
import os
import threading
from dataclasses import asdict
from pathlib import Path
from typing import Optional, Any
from datetime import datetime

from rocksq.blocking import PersistentQueueWithCapacity

from destinations.base import CDCRecord
from core.models import PipelineDestinationTableSync

logger = logging.getLogger(__name__)


class DLQMessage:
    """
    Dead Letter Queue message wrapper.

    Stores CDC record along with routing metadata for recovery.
    """

    def __init__(
        self,
        pipeline_id: int,
        source_id: int,
        destination_id: int,
        table_name: str,
        table_name_target: str,
        cdc_record: CDCRecord,
        table_sync_config: dict[str, Any],
        retry_count: int = 0,
        first_failed_at: Optional[str] = None,
    ):
        """
        Initialize DLQ message.

        Args:
            pipeline_id: Pipeline ID
            source_id: Source ID
            destination_id: Destination ID
            table_name: Source table name
            table_name_target: Target table name
            cdc_record: CDC record that failed
            table_sync_config: Serialized table sync configuration
            retry_count: Number of retry attempts
            first_failed_at: ISO timestamp when first failure occurred
        """
        self.pipeline_id = pipeline_id
        self.source_id = source_id
        self.destination_id = destination_id
        self.table_name = table_name
        self.table_name_target = table_name_target
        self.cdc_record = cdc_record
        self.table_sync_config = table_sync_config
        self.retry_count = retry_count
        self.first_failed_at = first_failed_at or datetime.utcnow().isoformat()

    def to_bytes(self) -> bytes:
        """Serialize message to bytes for storage."""
        data = {
            "pipeline_id": self.pipeline_id,
            "source_id": self.source_id,
            "destination_id": self.destination_id,
            "table_name": self.table_name,
            "table_name_target": self.table_name_target,
            "cdc_record": {
                "operation": self.cdc_record.operation,
                "table_name": self.cdc_record.table_name,
                "key": self.cdc_record.key,
                "value": self.cdc_record.value,
                "schema": self.cdc_record.schema,
                "timestamp": self.cdc_record.timestamp,
            },
            "table_sync_config": self.table_sync_config,
            "retry_count": self.retry_count,
            "first_failed_at": self.first_failed_at,
        }
        return json.dumps(data).encode("utf-8")

    @classmethod
    def from_bytes(cls, data: bytes) -> "DLQMessage":
        """Deserialize message from bytes."""
        obj = json.loads(data.decode("utf-8"))
        cdc_record = CDCRecord(**obj["cdc_record"])
        return cls(
            pipeline_id=obj["pipeline_id"],
            source_id=obj["source_id"],
            destination_id=obj["destination_id"],
            table_name=obj["table_name"],
            table_name_target=obj["table_name_target"],
            cdc_record=cdc_record,
            table_sync_config=obj["table_sync_config"],
            retry_count=obj["retry_count"],
            first_failed_at=obj["first_failed_at"],
        )


class DLQManager:
    """
    Manages persistent dead letter queues for CDC records.

    Uses RocksQ for persistent, disk-based queuing. Messages survive engine restarts.
    Queues are organized by: source_id/table_name/destination_id for granular control.
    """

    def __init__(self, base_path: str = "./tmp/dlq"):
        """
        Initialize DLQ manager.

        Args:
            base_path: Base directory for DLQ storage
        """
        self._base_path = Path(base_path)
        self._base_path.mkdir(parents=True, exist_ok=True)

        # Queue cache: (source_id, table_name, destination_id) -> PersistentQueueWithCapacity
        self._queues: dict[tuple[int, str, int], PersistentQueueWithCapacity] = {}
        self._queues_lock = threading.Lock()

        self._logger = logging.getLogger(f"{__name__}.DLQManager")
        self._logger.info(f"DLQ Manager initialized with base path: {self._base_path}")

    def _get_queue_path(
        self, source_id: int, table_name: str, destination_id: int
    ) -> Path:
        """
        Get queue directory path for specific source/table/destination.

        Format: base_path/source_{source_id}/table_{table_name}/dest_{destination_id}
        """
        return (
            self._base_path
            / f"source_{source_id}"
            / f"table_{table_name}"
            / f"dest_{destination_id}"
        )

    def _get_or_create_queue(
        self,
        source_id: int,
        table_name: str,
        destination_id: int,
    ) -> PersistentQueueWithCapacity:
        """
        Get or create persistent queue for specific source/table/destination.

        Thread-safe queue creation with caching.
        """
        key = (source_id, table_name, destination_id)

        # Fast path: queue already exists
        if key in self._queues:
            return self._queues[key]

        # Slow path: create new queue with lock
        with self._queues_lock:
            # Double-check after acquiring lock
            if key in self._queues:
                return self._queues[key]

            queue_path = self._get_queue_path(source_id, table_name, destination_id)
            queue_path.mkdir(parents=True, exist_ok=True)

            queue = PersistentQueueWithCapacity(str(queue_path))
            self._queues[key] = queue

            self._logger.info(
                f"Created DLQ queue: source_{source_id}/table_{table_name}/dest_{destination_id}"
            )

            return queue

    def enqueue(
        self,
        pipeline_id: int,
        source_id: int,
        destination_id: int,
        table_name: str,
        table_name_target: str,
        cdc_record: CDCRecord,
        table_sync: PipelineDestinationTableSync,
        error_message: Optional[str] = None,
    ) -> bool:
        """
        Add CDC record to dead letter queue.

        Args:
            pipeline_id: Pipeline ID
            source_id: Source database ID
            destination_id: Destination database ID
            table_name: Source table name
            table_name_target: Target table name
            cdc_record: CDC record that failed to write
            table_sync: Table sync configuration
            error_message: Optional error message

        Returns:
            True if successfully enqueued
        """
        try:
            # Serialize table_sync to dict for storage
            table_sync_dict = {
                "id": table_sync.id,
                "table_name": table_sync.table_name,
                "table_name_target": table_sync.table_name_target,
                "filter_sql": table_sync.filter_sql,
                "custom_sql": table_sync.custom_sql,
            }

            message = DLQMessage(
                pipeline_id=pipeline_id,
                source_id=source_id,
                destination_id=destination_id,
                table_name=table_name,
                table_name_target=table_name_target,
                cdc_record=cdc_record,
                table_sync_config=table_sync_dict,
            )

            queue = self._get_or_create_queue(source_id, table_name, destination_id)
            queue.push([message.to_bytes()], no_gil=True)

            self._logger.warning(
                f"Enqueued to DLQ: source_{source_id}/table_{table_name}/dest_{destination_id} "
                f"- operation={cdc_record.operation}, key={cdc_record.key}"
                + (f", error={error_message}" if error_message else "")
            )

            return True

        except Exception as e:
            self._logger.error(
                f"Failed to enqueue to DLQ: source_{source_id}/table_{table_name}/dest_{destination_id} - {e}",
                exc_info=True,
            )
            return False

    def dequeue_batch(
        self,
        source_id: int,
        table_name: str,
        destination_id: int,
        max_messages: int = 100,
    ) -> list[DLQMessage]:
        """
        Dequeue a batch of messages from specific queue.

        Args:
            source_id: Source database ID
            destination_id: Destination database ID
            table_name: Table name
            max_messages: Maximum number of messages to dequeue

        Returns:
            List of DLQ messages (empty if queue doesn't exist or is empty)
        """
        try:
            key = (source_id, table_name, destination_id)
            if key not in self._queues:
                # Queue doesn't exist yet, nothing to dequeue
                return []

            queue = self._queues[key]
            raw_messages = queue.pop(max_elements=max_messages, no_gil=True)

            if not raw_messages:
                return []

            messages = [DLQMessage.from_bytes(msg) for msg in raw_messages]

            self._logger.debug(
                f"Dequeued {len(messages)} messages from DLQ: "
                f"source_{source_id}/table_{table_name}/dest_{destination_id}"
            )

            return messages

        except Exception as e:
            self._logger.error(
                f"Failed to dequeue from DLQ: source_{source_id}/table_{table_name}/dest_{destination_id} - {e}",
                exc_info=True,
            )
            return []

    def get_queue_size(
        self,
        source_id: int,
        table_name: str,
        destination_id: int,
    ) -> int:
        """
        Get approximate size of specific queue.

        Returns:
            Number of messages in queue (0 if queue doesn't exist)
        """
        try:
            key = (source_id, table_name, destination_id)
            if key not in self._queues:
                # Check if queue directory exists on disk
                queue_path = self._get_queue_path(source_id, table_name, destination_id)
                if not queue_path.exists():
                    return 0
                # Queue exists on disk but not loaded yet
                queue = self._get_or_create_queue(source_id, table_name, destination_id)
            else:
                queue = self._queues[key]

            # RocksQ doesn't provide size() method directly, so we peek
            # This is approximate - actual implementation may vary
            return 0  # Placeholder - rocksq 0.3.0 may not expose size easily

        except Exception as e:
            self._logger.error(f"Failed to get queue size: {e}")
            return 0

    def list_queues(self) -> list[tuple[int, str, int]]:
        """
        List all queue identifiers.

        Returns:
            List of (source_id, table_name, destination_id) tuples
        """
        queues = []
        try:
            # Scan filesystem for queue directories
            if not self._base_path.exists():
                return []

            for source_dir in self._base_path.iterdir():
                if not source_dir.is_dir() or not source_dir.name.startswith("source_"):
                    continue

                source_id = int(source_dir.name.replace("source_", ""))

                for table_dir in source_dir.iterdir():
                    if not table_dir.is_dir() or not table_dir.name.startswith(
                        "table_"
                    ):
                        continue

                    table_name = table_dir.name.replace("table_", "")

                    for dest_dir in table_dir.iterdir():
                        if not dest_dir.is_dir() or not dest_dir.name.startswith(
                            "dest_"
                        ):
                            continue

                        destination_id = int(dest_dir.name.replace("dest_", ""))
                        queues.append((source_id, table_name, destination_id))

            return queues

        except Exception as e:
            self._logger.error(f"Failed to list queues: {e}")
            return []

    def has_messages(
        self,
        source_id: int,
        table_name: str,
        destination_id: int,
    ) -> bool:
        """
        Check if queue has any messages.

        This is done by attempting to peek without consuming.
        """
        try:
            key = (source_id, table_name, destination_id)
            if key not in self._queues:
                queue_path = self._get_queue_path(source_id, table_name, destination_id)
                if not queue_path.exists():
                    return False
                # Load queue to check
                self._get_or_create_queue(source_id, table_name, destination_id)

            # Try to pop and re-push to check if queue has messages
            queue = self._queues[key]
            messages = queue.pop(max_elements=1, no_gil=True)
            if messages:
                # Re-push the message back
                queue.push(messages, no_gil=True)
                return True
            return False

        except Exception as e:
            self._logger.error(f"Failed to check queue: {e}")
            return False

    def close_all(self) -> None:
        """Close all open queues and cleanup resources."""
        with self._queues_lock:
            self._queues.clear()
            self._logger.info("Closed all DLQ queues")
