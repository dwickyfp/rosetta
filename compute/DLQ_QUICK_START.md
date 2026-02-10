# DLQ Quick Start Guide

## Installation

### 1. Install Dependencies

```bash
cd compute
pip install -r requirements.txt
```

This will install `rocksq==0.3.0` along with other dependencies.

### 2. Verify Installation

```bash
python -c "import rocksq; print(f'rocksq version: {rocksq.__version__}')"
```

Expected output: `rocksq version: 0.3.0`

## Configuration

### 3. Set Environment Variables (Optional)

Create or update `.env` file:

```bash
# DLQ Configuration (optional - defaults shown)
DLQ_BASE_PATH=./tmp/dlq
DLQ_CHECK_INTERVAL=30
DLQ_BATCH_SIZE=100
```

### 4. Verify Configuration

```bash
python -c "from config import get_config; c = get_config(); print(f'DLQ path: {c.dlq.base_path}')"
```

## Testing

### 5. Run Example Script

```bash
cd compute
python examples/dlq_example.py
```

Expected output:

```
============================================================
DLQ Example and Test Script
============================================================

=== Example 1: Enqueue Messages ===

Enqueued record 1: ✓
Enqueued record 2: ✓

Enqueued 2 messages to DLQ

=== Example 2: List Queues ===

Found 1 queue(s):
  - source_1/table_users/dest_2: 📦 Has messages

=== Example 3: Dequeue Messages ===

Dequeued 2 message(s):

Message 1:
  Pipeline ID: 1
  Source ID: 1
  Destination ID: 2
  Table: users → USERS
  Operation: u
  Key: {'id': 1}
  Value: {'id': 1, 'name': 'Alice', 'email': 'alice@example.com'}
  Retry count: 0
  First failed at: 2024-02-10T...

...

✓ Cleaned up test directory: ./tmp/dlq_test
```

## Integration Test

### 6. Test with Real Pipeline

#### Start Pipeline

```bash
cd compute
python main.py
```

#### Simulate Destination Failure

In another terminal:

```bash
# For PostgreSQL destination
docker stop rosetta-postgres-dest

# For Snowflake, temporarily change credentials to invalid
```

#### Generate CDC Events

Make changes to source database (inserts, updates, deletes)

#### Check DLQ

```bash
# View DLQ directory structure
ls -lR tmp/dlq/

# Monitor logs for DLQ activity
tail -f logs/compute.log | grep "DLQ\|Enqueued"
```

Expected log output:

```
2024-02-10 12:34:56 - INFO - DLQ Manager initialized with base path: ./tmp/dlq
2024-02-10 12:34:56 - INFO - DLQ recovery worker started
2024-02-10 12:35:10 - WARNING - ✗ Failed to write to destination PostgreSQL-Prod
2024-02-10 12:35:10 - WARNING - Enqueued to DLQ: source_1/table_users/dest_1 - operation=u, key={'id': 123}
```

#### Restore Destination

```bash
# Restart destination
docker start rosetta-postgres-dest

# Or fix Snowflake credentials
```

#### Verify Recovery

Watch logs for automatic replay:

```bash
tail -f logs/compute.log | grep "replayed\|HEALTHY"
```

Expected output:

```
2024-02-10 12:36:30 - INFO - ✓ Destination PostgreSQL-Prod (ID=1) is now HEALTHY - will attempt DLQ recovery
2024-02-10 12:36:30 - INFO - Processing 5 DLQ messages for source_1/table_users/dest_1
2024-02-10 12:36:31 - INFO - ✓ Successfully replayed 5/5 DLQ messages to PostgreSQL-Prod for table users
```

#### Verify DLQ Empty

```bash
ls -lR tmp/dlq/
# Should show empty or no queue directories
```

## Verification Checklist

- [x] rocksq installed successfully
- [x] DLQ configuration loaded
- [x] Example script runs without errors
- [x] DLQ directory created (`./tmp/dlq/`)
- [x] Messages enqueued on destination failure
- [x] Recovery worker detects healthy destination
- [x] Messages replayed successfully
- [x] DLQ emptied after replay

## Troubleshooting

### rocksq Installation Failed

**Problem**: `pip install rocksq==0.3.0` fails

**Solutions**:

1. Ensure Python 3.8+ is installed
2. Update pip: `pip install --upgrade pip`
3. Install build tools:
   - Linux: `sudo apt-get install python3-dev`
   - Mac: `xcode-select --install`
   - Windows: Install Visual C++ Build Tools

### DLQ Not Initialized

**Problem**: Logs don't show "DLQ Manager initialized"

**Check**:

```bash
# Verify imports work
python -c "from core.dlq_manager import DLQManager; print('✓ Import OK')"

# Check config
python -c "from config import get_config; print(get_config().dlq)"
```

### Messages Not Enqueued

**Problem**: Destination fails but no DLQ messages

**Debug**:

```bash
# Check if DLQ manager is passed to event handler
grep "dlq_manager" compute/core/engine.py

# Verify event handler has DLQ integration
grep "_enqueue_to_dlq" compute/core/event_handler.py

# Check logs for errors
tail -f logs/compute.log | grep -i "error.*dlq"
```

### Recovery Not Working

**Problem**: Messages in DLQ but not replayed

**Debug**:

```bash
# Check recovery worker started
grep "DLQ recovery worker started" logs/compute.log

# Check health checks
grep "connection test" logs/compute.log

# Manually test destination connection
python -c "
from destinations.postgresql import PostgreSQLDestination
from core.models import Destination
# Create mock config and test
"
```

## Performance Tuning

### High-Volume Scenarios

```bash
# Increase batch size and check frequency
export DLQ_BATCH_SIZE=500
export DLQ_CHECK_INTERVAL=10
```

### Low-Latency Scenarios

```bash
# Smaller batches, less frequent checks
export DLQ_BATCH_SIZE=50
export DLQ_CHECK_INTERVAL=60
```

### Disk Space

```bash
# Monitor DLQ disk usage
du -sh tmp/dlq/

# Set up monitoring alert
watch -n 60 'du -sh tmp/dlq/'
```

## Next Steps

1. **Production Deployment**
   - Set `DLQ_BASE_PATH` to dedicated volume
   - Configure monitoring alerts
   - Set up log aggregation
   - Plan backup strategy

2. **Monitoring Setup**
   - Track queue sizes
   - Alert on growth trends
   - Monitor replay rates
   - Track retry counts

3. **Testing**
   - Load test with high CDC volume
   - Test prolonged destination outages
   - Verify recovery under load
   - Test engine restarts

4. **Documentation**
   - Update runbooks
   - Document failure scenarios
   - Create dashboards
   - Train operations team

## References

- **Full Documentation**: [DLQ_FEATURE.md](./DLQ_FEATURE.md)
- **Implementation Summary**: [DLQ_IMPLEMENTATION_SUMMARY.md](./DLQ_IMPLEMENTATION_SUMMARY.md)
- **Example Script**: [examples/dlq_example.py](./examples/dlq_example.py)
- **RocksQ Documentation**: https://github.com/rocksq/rocksq

## Support

For questions or issues:

1. Review logs: `tail -f logs/compute.log`
2. Check queue status: `ls -lR tmp/dlq/`
3. Run diagnostics: `python examples/dlq_example.py`
4. Review documentation above

## Success Indicators

Your DLQ implementation is working correctly if:

✅ Example script runs successfully  
✅ DLQ directory created automatically  
✅ Destination failures trigger enqueue  
✅ Logs show "Enqueued to DLQ" on failures  
✅ Recovery worker starts with pipeline  
✅ Health checks run every 30 seconds  
✅ Messages replay when destination recovers  
✅ Logs show "Successfully replayed" messages  
✅ DLQ queues empty after recovery  
✅ No error messages in logs

**Congratulations! Your DLQ system is operational.**
