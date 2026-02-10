#!/usr/bin/env python3
"""
DLQ Example and Test Script

Demonstrates basic DLQ functionality including:
- Enqueue messages
- Check queue status
- Manual dequeue
- Simulate recovery flow
"""

import sys
import os

# Add compute to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from core.dlq_manager import DLQManager, DLQMessage
from destinations.base import CDCRecord


def example_enqueue():
    """Example: Enqueue messages to DLQ."""
    print("\n=== Example 1: Enqueue Messages ===\n")

    dlq = DLQManager(base_path="./tmp/dlq_test")

    # Create sample CDC records
    records = [
        CDCRecord(
            operation="u",
            table_name="users",
            key={"id": 1},
            value={"id": 1, "name": "Alice", "email": "alice@example.com"},
            timestamp=1678901234567,
        ),
        CDCRecord(
            operation="c",
            table_name="users",
            key={"id": 2},
            value={"id": 2, "name": "Bob", "email": "bob@example.com"},
            timestamp=1678901234568,
        ),
    ]

    # Mock table sync config
    table_sync_config = {
        "id": 1,
        "table_name": "users",
        "table_name_target": "USERS",
        "filter_sql": None,
        "custom_sql": None,
    }

    # Create mock table sync object
    class TableSyncMock:
        def __init__(self, config):
            for k, v in config.items():
                setattr(self, k, v)

    table_sync = TableSyncMock(table_sync_config)

    # Enqueue messages
    for i, record in enumerate(records, 1):
        success = dlq.enqueue(
            pipeline_id=1,
            source_id=1,
            destination_id=2,
            table_name="users",
            table_name_target="USERS",
            cdc_record=record,
            table_sync=table_sync,
            error_message=f"Test error {i}: Connection refused",
        )
        print(f"Enqueued record {i}: {'✓' if success else '✗'}")

    print(f"\nEnqueued {len(records)} messages to DLQ")


def example_list_queues():
    """Example: List all DLQ queues."""
    print("\n=== Example 2: List Queues ===\n")

    dlq = DLQManager(base_path="./tmp/dlq_test")
    queues = dlq.list_queues()

    if not queues:
        print("No queues found")
        return

    print(f"Found {len(queues)} queue(s):")
    for source_id, table_name, dest_id in queues:
        has_msgs = dlq.has_messages(source_id, table_name, dest_id)
        status = "📦 Has messages" if has_msgs else "✓ Empty"
        print(f"  - source_{source_id}/table_{table_name}/dest_{dest_id}: {status}")


def example_dequeue():
    """Example: Dequeue and inspect messages."""
    print("\n=== Example 3: Dequeue Messages ===\n")

    dlq = DLQManager(base_path="./tmp/dlq_test")

    # Dequeue from specific queue
    messages = dlq.dequeue_batch(
        source_id=1,
        table_name="users",
        destination_id=2,
        max_messages=10,
    )

    if not messages:
        print("No messages in queue")
        return

    print(f"Dequeued {len(messages)} message(s):\n")

    for i, msg in enumerate(messages, 1):
        print(f"Message {i}:")
        print(f"  Pipeline ID: {msg.pipeline_id}")
        print(f"  Source ID: {msg.source_id}")
        print(f"  Destination ID: {msg.destination_id}")
        print(f"  Table: {msg.table_name} → {msg.table_name_target}")
        print(f"  Operation: {msg.cdc_record.operation}")
        print(f"  Key: {msg.cdc_record.key}")
        print(f"  Value: {msg.cdc_record.value}")
        print(f"  Retry count: {msg.retry_count}")
        print(f"  First failed at: {msg.first_failed_at}")
        print()


def example_check_status():
    """Example: Check queue status without dequeuing."""
    print("\n=== Example 4: Check Queue Status ===\n")

    dlq = DLQManager(base_path="./tmp/dlq_test")

    queues = dlq.list_queues()
    if not queues:
        print("No queues found")
        return

    for source_id, table_name, dest_id in queues:
        has_msgs = dlq.has_messages(source_id, table_name, dest_id)
        print(f"Queue: source_{source_id}/table_{table_name}/dest_{dest_id}")
        print(f"  Has messages: {has_msgs}")

        if has_msgs:
            # Peek at one message
            msgs = dlq.dequeue_batch(source_id, table_name, dest_id, max_messages=1)
            if msgs:
                msg = msgs[0]
                print(f"  First message operation: {msg.cdc_record.operation}")
                print(f"  First message key: {msg.cdc_record.key}")
                # Re-enqueue for next check
                queue = dlq._get_or_create_queue(source_id, table_name, dest_id)
                queue.push([msg.to_bytes()], no_gil=True)
        print()


def cleanup():
    """Cleanup test DLQ directory."""
    import shutil

    dlq_path = "./tmp/dlq_test"
    if os.path.exists(dlq_path):
        shutil.rmtree(dlq_path)
        print(f"\n✓ Cleaned up test directory: {dlq_path}")


def main():
    """Run all examples."""
    print("=" * 60)
    print("DLQ Example and Test Script")
    print("=" * 60)

    try:
        # Run examples
        example_enqueue()
        example_list_queues()
        example_dequeue()

        # Enqueue again for check status example
        print("\n--- Re-enqueueing for status check example ---")
        example_enqueue()
        example_check_status()

        print("\n" + "=" * 60)
        print("All examples completed successfully!")
        print("=" * 60)

    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback

        traceback.print_exc()

    finally:
        # Cleanup
        cleanup()


if __name__ == "__main__":
    main()
