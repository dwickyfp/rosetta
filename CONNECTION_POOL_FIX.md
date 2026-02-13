# Connection Pool Exhaustion Fix - Production Patch

## Problem Description

Production encountered two critical issues:

```
2026-02-13 10:02:00 - core.backfill_manager - ERROR - Error fetching pending jobs (attempt 1/3): connection pool exhausted
2026-02-13 10:02:04 - core.manager - ERROR - Error syncing pipeline states: 'ThreadedConnectionPool' object has no attribute 'PoolError'
```

### Root Causes

1. **AttributeError**: Incorrect exception handling
   - Code tried to catch `pool.PoolError` where `pool` was a `ThreadedConnectionPool` instance
   - Should have been `psycopg2.pool.PoolError`

2. **Connection Pool Exhaustion**: All 10 connections in use
   - Connections not being returned to pool properly
   - No retry logic when pool exhausted
   - `_get_pending_jobs()` used direct `pool.getconn()` without retry

## Solutions Applied

### 1. Fixed PoolError Exception Reference (database.py)

Changed from:

```python
except pool.PoolError as e:  # WRONG: pool is ThreadedConnectionPool instance
```

To:

```python
except psycopg2.pool.PoolError as e:  # CORRECT: import from psycopg2
```

**Location**: `compute/core/database.py` - `get_db_connection()` function

### 2. Added Retry Logic for Pool Exhaustion (database.py)

Implemented 3-attempt retry with exponential backoff when connection pool is exhausted:

```python
def get_db_connection() -> psycopg2.extensions.connection:
    connection_pool = get_connection_pool()
    max_retries = 3
    retry_delay = 0.5  # Start with 500ms

    for attempt in range(max_retries):
        try:
            conn = connection_pool.getconn()
            # ... validation ...
            return conn
        except psycopg2.pool.PoolError as e:
            if attempt < max_retries - 1:
                logger.warning(f"Connection pool exhausted (attempt {attempt + 1}/{max_retries}), retrying in {retry_delay:.2f}s...")
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff: 0.5s, 1s, 2s
            else:
                raise DatabaseException(f"Failed to get connection: {e}")
```

**Benefit**: Automatically retries when all 10 connections are temporarily in use

### 3. Updated Backfill Manager Methods (backfill_manager.py)

Replaced all direct `pool.getconn()` calls with `get_db_connection()` to leverage built-in retry logic:

**Methods updated**:

- `_recover_stale_jobs()`
- `_get_pending_jobs()` (simplified from manual retry to use built-in logic)
- `_update_job_status()`
- `_update_job_count()`
- `_update_job_total_record()`
- `_is_job_cancelled()`

**Before**:

```python
pool = get_connection_pool()
conn = pool.getconn()  # No retry on pool exhaustion!
```

**After**:

```python
conn = get_db_connection()  # Automatic retry with exponential backoff
```

### 4. Improved Connection Cleanup

All methods now properly clean up connections:

```python
finally:
    if conn:
        from core.database import return_db_connection
        try:
            return_db_connection(conn)
        except Exception as e:
            logger.warning(f"Error returning connection to pool: {e}")
```

## Retry Strategy

### Connection Pool Exhaustion

- **Attempts**: 3 retries
- **Delays**: 0.5s → 1s → 2s (exponential backoff)
- **Total wait**: Up to 3.5 seconds

### Connection Pool Initialization (from previous fix)

- **Attempts**: 5 retries
- **Delays**: 2s → 3s → 4.5s → 6.75s → 10s (exponential backoff)
- **Total wait**: Up to ~27 seconds for Docker startup timing

## Flow Diagram

```
Application Request for Connection
    ↓
get_db_connection()
    ├─ Attempt 1: Try pool.getconn()
    │   ├─ Success: Validate with SELECT 1
    │   │   └─ Return connection ✓
    │   └─ PoolError: Log warning, wait 0.5s, retry
    ├─ Attempt 2: Try pool.getconn()
    │   ├─ Success: Return connection ✓
    │   └─ PoolError: Log warning, wait 1s, retry
    ├─ Attempt 3: Try pool.getconn()
    │   ├─ Success: Return connection ✓
    │   └─ PoolError: Log error, raise exception
    └─ Finally: Ensure connection returned to pool
```

## Testing Recommendations

### Test 1: Pool Exhaustion Recovery

```bash
# Monitor logs during high concurrency
docker-compose logs -f compute | grep "pool exhausted"
```

Expected: Few "pool exhausted" warnings followed by "Successfully fetched jobs"

### Test 2: Connection Cleanup

```bash
# Check active connections
docker-compose exec postgres psql -U replicator -d config_db \
  -c "SELECT count(*) FROM pg_stat_activity WHERE datname='config_db';"
```

Expected: Should stay below 10 connections per pipeline

### Test 3: Backfill Job Execution

```bash
# Verify backfill jobs process without errors
docker-compose logs compute | grep -E "(backfill|pending jobs)" | tail -10
```

Expected: Regular successful job processing

## Metrics & Monitoring

### Success Indicators

- No "connection pool exhausted" errors in logs
- No "PoolError: 'ThreadedConnectionPool' has no attribute" errors
- Database connection count stays stable (<10 per pipeline)
- Backfill jobs complete successfully

### Warning Signs

- Multiple "pool exhausted" messages per minute
- "Error syncing pipeline states" errors increasing
- Database connection count reaching 10+

## Files Modified

1. **compute/core/database.py**
   - Fixed `PoolError` exception reference
   - Added retry logic to `get_db_connection()`

2. **compute/core/backfill_manager.py**
   - Added import: `from core.database import get_db_connection`
   - Updated 6 methods to use `get_db_connection()` instead of direct `pool.getconn()`
   - Improved connection cleanup in all methods

## Deployment Instructions

1. **Rebuild Docker image**:

   ```bash
   docker build -t rosetta:latest .
   ```

2. **Deploy**:

   ```bash
   docker-compose down
   docker-compose up -d
   ```

3. **Monitor initialization**:

   ```bash
   docker-compose logs -f compute | head -30
   ```

   Look for:

   ```
   ✓ Connection pool initialized successfully on attempt 1
   ✓ Backfill manager started
   ✓ Pipeline manager started with X active pipelines
   ```

   NOT:

   ```
   ✗ Error fetching pending jobs: 'ThreadedConnectionPool' has no attribute 'PoolError'
   ✗ connection pool exhausted (without recovery)
   ```

## Backwards Compatibility

✓ No breaking changes
✓ All existing code patterns still work
✓ Transparent retry logic (application code doesn't change)
✓ Better resource cleanup (returns invalid connections before getting new ones)

## Performance Impact

- **Minimal**: Retry delays only occur when pool exhausted (rare after fix)
- **Positive**: Fewer failed requests due to transient pool exhaustion
- **Cost**: 0.5-2ms additional delay per retry (far less than request timeout)

## Future Improvements

1. Monitor pool exhaustion frequency and adjust pool size if needed
2. Add metrics export for connection pool utilization
3. Consider implementing connection pool pre-warming on startup
4. Add configurable pool exhaustion retry attempts via environment variable
