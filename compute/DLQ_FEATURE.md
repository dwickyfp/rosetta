# Dead Letter Queue (DLQ) Feature

## Overview

The Dead Letter Queue (DLQ) feature provides persistent message queuing for CDC records that fail to reach their destinations. This ensures data durability and automatic recovery when destination connections are restored.

## Architecture

### Components

1. **DLQManager** (`core/dlq_manager.py`)
   - Manages persistent RocksQ queues organized by source/table/destination
   - Handles message serialization/deserialization
   - Provides queue operations: enqueue, dequeue, list, check existence

2. **DLQRecoveryWorker** (`core/dlq_recovery.py`)
   - Background thread that monitors destination health
   - Automatically replays DLQ messages when destinations recover
   - Configurable check interval and batch size

3. **Integration Points**
   - **Event Handler**: Automatically enqueues failed writes to DLQ
   - **Pipeline Engine**: Initializes DLQ components and manages lifecycle
   - **Destinations**: Implement `test_connection()` for health checks

## How It Works

### Normal Flow (No Failures)

```
CDC Event → Event Handler → Destination Write → Success ✓
```

### Failure Flow (DLQ Activation)

```
CDC Event → Event Handler → Destination Write → Failed ✗
                                ↓
                          Enqueue to DLQ
                                ↓
                    Persistent Storage (RocksQ)
```

### Recovery Flow

```
DLQ Recovery Worker (Background Thread)
    ↓
Check Destination Health (every 30s by default)
    ↓
If Healthy: Dequeue Batch → Replay to Destination
    ↓
Success: Remove from DLQ
Failed: Re-enqueue with retry counter
```

## Queue Organization

DLQ messages are organized in a hierarchical directory structure:

```
./tmp/dlq/
├── source_1/
│   ├── table_users/
│   │   ├── dest_1/     # PostgreSQL destination
│   │   └── dest_2/     # Snowflake destination
│   └── table_orders/
│       └── dest_1/
└── source_2/
    └── table_products/
        └── dest_1/
```

**Benefits:**

- Granular control per source/table/destination combination
- Easy monitoring and debugging
- Independent recovery per queue
- Persistent across engine restarts

## Configuration

### Environment Variables

Add to `.env` file:

```bash
# DLQ Configuration
DLQ_BASE_PATH=./tmp/dlq              # Base directory for DLQ storage
DLQ_CHECK_INTERVAL=30                # Seconds between recovery attempts
DLQ_BATCH_SIZE=100                   # Messages to process per batch
```

### Programmatic Configuration

Configuration is loaded via `config/config.py`:

```python
@dataclass
class DLQConfig:
    base_path: str = "./tmp/dlq"
    check_interval: int = 30
    batch_size: int = 100
```

## Message Format

Each DLQ message contains:

```python
{
    "pipeline_id": 1,
    "source_id": 1,
    "destination_id": 2,
    "table_name": "users",
    "table_name_target": "USERS",
    "cdc_record": {
        "operation": "u",
        "table_name": "users",
        "key": {"id": 123},
        "value": {"id": 123, "name": "John", ...},
        "schema": {...},
        "timestamp": 1678901234567
    },
    "table_sync_config": {
        "id": 1,
        "table_name": "users",
        "table_name_target": "USERS",
        "filter_sql": null,
        "custom_sql": null
    },
    "retry_count": 0,
    "first_failed_at": "2024-02-10T12:34:56.789Z"
}
```

## Usage Examples

### Automatic Usage (Default Behavior)

DLQ is automatically activated when destinations fail:

```python
# In event handler - happens automatically
try:
    written = destination.write_batch(records, table_sync)
except Exception as e:
    # Automatically enqueues to DLQ
    if self._dlq_manager:
        self._enqueue_to_dlq(records, routing, str(e))
```

### Manual DLQ Operations

#### Check Queue Status

```python
from core.dlq_manager import DLQManager

dlq = DLQManager(base_path="./tmp/dlq")

# List all queues
queues = dlq.list_queues()
for source_id, table_name, dest_id in queues:
    has_msgs = dlq.has_messages(source_id, table_name, dest_id)
    print(f"Queue source_{source_id}/table_{table_name}/dest_{dest_id}: {has_msgs}")
```

#### Manual Enqueue (Testing)

```python
from core.dlq_manager import DLQManager
from destinations.base import CDCRecord

dlq = DLQManager()

record = CDCRecord(
    operation="u",
    table_name="users",
    key={"id": 123},
    value={"id": 123, "name": "John"},
)

dlq.enqueue(
    pipeline_id=1,
    source_id=1,
    destination_id=2,
    table_name="users",
    table_name_target="USERS",
    cdc_record=record,
    table_sync=table_sync_obj,
)
```

#### Manual Dequeue (Testing)

```python
messages = dlq.dequeue_batch(
    source_id=1,
    table_name="users",
    destination_id=2,
    max_messages=10,
)

for msg in messages:
    print(f"Operation: {msg.cdc_record.operation}")
    print(f"Key: {msg.cdc_record.key}")
    print(f"Retry count: {msg.retry_count}")
```

## Health Checks

### Destination Health Monitoring

Each destination implements `test_connection()`:

**PostgreSQL:**

```python
def test_connection(self) -> bool:
    try:
        conn = psycopg2.connect(..., connect_timeout=5)
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1")
        conn.close()
        return True
    except:
        return False
```

**Snowflake:**

```python
def test_connection(self) -> bool:
    try:
        # Execute simple query
        result = await client.execute_query("SELECT CURRENT_VERSION()", timeout=5)
        return result is not None
    except:
        return False
```

### Health Check Frequency

- Default: Every 30 seconds (configurable via `DLQ_CHECK_INTERVAL`)
- Lightweight checks to avoid overhead
- State changes logged: `UNHEALTHY → HEALTHY` triggers recovery

## Monitoring and Logging

### Log Levels

**INFO:** Recovery operations, state changes

```
INFO - DLQ Manager initialized with base path: ./tmp/dlq
INFO - ✓ Destination PostgreSQL-Prod (ID=1) is now HEALTHY - will attempt DLQ recovery
INFO - ✓ Successfully replayed 50/50 DLQ messages to PostgreSQL-Prod for table users
```

**WARNING:** Failed writes, re-enqueue

```
WARNING - Enqueued to DLQ: source_1/table_users/dest_2 - operation=u, key={'id': 123}
WARNING - Failed to replay DLQ messages to Snowflake-Prod for table orders: Connection timeout
```

**ERROR:** Critical issues

```
ERROR - Failed to enqueue to DLQ: source_1/table_users/dest_2 - Disk full
```

### Recovery Worker Statistics

```python
stats = dlq_recovery_worker.get_stats()
# Returns:
{
    "running": True,
    "pipeline_id": 1,
    "pipeline_name": "my-pipeline",
    "check_interval": 30,
    "batch_size": 100,
    "total_queues": 5,
    "destination_health": {
        1: True,   # PostgreSQL healthy
        2: False,  # Snowflake unhealthy
    }
}
```

## Persistence Guarantees

### Data Durability

- **RocksQ**: LSM-tree based persistent queue (RocksDB backend)
- **Write-Ahead Log**: Messages written to disk before acknowledgment
- **Crash Recovery**: Queues survive engine restarts
- **Atomic Operations**: Push/pop are atomic at queue level

### Storage Location

Default: `./tmp/dlq/`

**Production Recommendation:**

- Use dedicated disk/volume with monitoring
- Set appropriate disk quotas
- Regular backups (optional, for critical data)

### Disk Space Management

RocksQ automatically manages storage:

- Compaction for space efficiency
- Old entries removed after successful replay
- No automatic expiry (messages stay until successfully replayed)

## Performance Considerations

### Throughput

- **Normal Path (No DLQ)**: No overhead
- **DLQ Write**: ~1-2ms per message (async disk write)
- **Recovery**: Batched (100 messages/batch default)

### Memory Usage

- Minimal memory footprint (queues are disk-based)
- In-memory cache for open queue handles
- No large buffers kept in memory

### Tuning Parameters

```bash
# High-throughput scenarios
DLQ_BATCH_SIZE=500              # Larger batches
DLQ_CHECK_INTERVAL=10           # More frequent checks

# Low-latency scenarios
DLQ_BATCH_SIZE=50               # Smaller batches
DLQ_CHECK_INTERVAL=60           # Less frequent checks
```

## Error Handling

### Retry Strategy

1. **Immediate Retry**: First failure → enqueue to DLQ
2. **Background Retry**: Recovery worker attempts replay every `check_interval`
3. **Persistent Retry**: Messages stay in DLQ until successful (no max retries)
4. **Retry Counter**: Tracked in `retry_count` field (for monitoring)

### Failure Scenarios

| Scenario            | Behavior                                     |
| ------------------- | -------------------------------------------- |
| Destination down    | Enqueue to DLQ, wait for recovery            |
| Network timeout     | Enqueue to DLQ, automatic retry              |
| Schema mismatch     | Enqueue to DLQ, requires manual intervention |
| Authentication fail | Enqueue to DLQ, logs error details           |
| Disk full (DLQ)     | Log critical error, drop message (rare)      |

### Manual Intervention

For persistent failures (e.g., schema issues):

1. Check DLQ logs for error details
2. Fix underlying issue (schema, credentials, etc.)
3. Wait for automatic recovery, or
4. Manually replay messages (see Usage Examples)

## Testing

### Unit Tests

Located in `compute/tests/`:

```bash
# Run DLQ tests
pytest tests/test_dlq_manager.py
pytest tests/test_dlq_recovery.py
```

### Integration Test

Simulate destination failure:

```python
# 1. Start pipeline
# 2. Stop destination database
# 3. Generate CDC events → Should enqueue to DLQ
# 4. Restart destination database
# 5. Verify DLQ messages are replayed automatically
```

### Manual Testing

```bash
# 1. Start compute engine
python main.py

# 2. Monitor DLQ directory
watch -n 1 'ls -lR ./tmp/dlq/'

# 3. Simulate failure (kill destination DB)
docker stop rosetta-snowflake

# 4. Check logs for DLQ enqueue
tail -f logs/compute.log | grep "Enqueued to DLQ"

# 5. Restart destination
docker start rosetta-snowflake

# 6. Verify recovery
tail -f logs/compute.log | grep "Successfully replayed"
```

## Troubleshooting

### DLQ Not Working

**Check 1: DLQ Manager Initialized?**

```
grep "DLQ manager initialized" logs/compute.log
```

**Check 2: Recovery Worker Started?**

```
grep "DLQ recovery worker started" logs/compute.log
```

**Check 3: Messages Enqueued?**

```
ls -lR ./tmp/dlq/
```

### Messages Not Replaying

**Check 1: Destination Health**

```
grep "Destination.*is now HEALTHY" logs/compute.log
```

**Check 2: Health Check Failing?**

```
grep "connection test failed" logs/compute.log
```

**Check 3: Recovery Worker Running?**

```
grep "Processing.*DLQ messages" logs/compute.log
```

### High DLQ Backlog

**Cause:** Destination slow or intermittent failures

**Solutions:**

- Increase `DLQ_BATCH_SIZE` for faster replay
- Decrease `DLQ_CHECK_INTERVAL` for more frequent attempts
- Investigate destination performance issues
- Consider scaling destinations

## Best Practices

1. **Monitor DLQ Size**: Set up alerts for growing queues
2. **Disk Space**: Ensure adequate disk for `DLQ_BASE_PATH`
3. **Health Checks**: Keep `test_connection()` lightweight (<1s)
4. **Logging**: Monitor logs for `"Enqueued to DLQ"` patterns
5. **Testing**: Regularly test failure/recovery scenarios
6. **Retention**: DLQ messages stay until replayed (plan accordingly)

## Future Enhancements

Potential improvements:

- [ ] Message expiry/TTL configuration
- [ ] DLQ metrics endpoint (Prometheus)
- [ ] Web UI for DLQ monitoring
- [ ] Manual replay via API
- [ ] Priority queues for critical tables
- [ ] Backpressure mechanism
- [ ] Dead letter queue for DLQ (recursive failures)

## Dependencies

- **rocksq==0.3.0**: Persistent queue implementation
- Compatible with Python 3.8+
- No additional runtime dependencies

## License

Same as Rosetta project.
