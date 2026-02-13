# Code Changes Summary - Production Fixes

## Files Modified

### 1. compute/core/database.py

**Function**: `init_connection_pool()`
**Change**: Added 5-attempt retry logic with exponential backoff

**Key additions**:

- Retry loop: 5 attempts
- Exponential backoff: 2s → 3s → 4.5s → 6.75s → 10s
- Logging: WARNING per attempt, ERROR on final failure
- Import requirement: `import time` (line 8)

**Before**:

```python
try:
    # ... setup DSN ...
    _connection_pool = pool.ThreadedConnectionPool(
        minconn=min_conn, maxconn=max_conn, **dsn
    )
    return _connection_pool
except psycopg2.Error as e:
    raise DatabaseException(...)
```

**After**:

```python
max_retries = 5
retry_delay = 2.0
last_error = None

for attempt in range(max_retries):
    try:
        # ... setup DSN ...
        _connection_pool = pool.ThreadedConnectionPool(
            minconn=min_conn, maxconn=max_conn, **dsn
        )
        logger.info(f"Connection pool initialized successfully on attempt {attempt + 1}")
        return _connection_pool
    except psycopg2.Error as e:
        last_error = e
        if attempt < max_retries - 1:
            logger.warning(
                f"Failed to initialize connection pool (attempt {attempt + 1}/{max_retries}): {e}. "
                f"Retrying in {retry_delay:.1f}s..."
            )
            time.sleep(retry_delay)
            retry_delay *= 1.5  # Exponential backoff
        else:
            logger.error(f"Failed to initialize connection pool after {max_retries} attempts: {e}")

raise DatabaseException(f"Failed to initialize connection pool: {last_error}")
```

**Function**: `get_db_connection()`
**Change**: Improved connection health validation with actual SQL query

**Key additions**:

- `SELECT 1` query to validate connection is alive
- Catches `psycopg2.OperationalError` and `psycopg2.InterfaceError`
- Removes dead connections with `close=True` flag
- Retries with fresh connection

**Code segment**:

```python
conn = pool.getconn()

# Validate connection is alive with a simple query
try:
    with conn.cursor() as cur:
        cur.execute("SELECT 1")  # Simple health check
except (psycopg2.OperationalError, psycopg2.InterfaceError):
    # Connection is dead, remove it and get a new one
    logger.warning("Detected dead connection, removing from pool")
    pool.putconn(conn, close=True)
    conn = pool.getconn()

return conn
```

---

### 2. compute/core/backfill_manager.py

**Function**: `_get_pending_jobs()`
**Change**: Enhanced retry logic and error handling

**Key improvements**:

- Separate exception handling for `psycopg2.OperationalError` (network failures)
- Exponential backoff: 2s → 4s → 8s
- Proper connection cleanup: `pool.putconn(conn, close=True)` on network errors
- Detailed logging per attempt

**Code segment** (lines 210-280):

```python
max_retries = 3
retry_delay = 2  # Start with 2 seconds
for attempt in range(max_retries):
    pool = None
    conn = None
    try:
        pool = get_connection_pool()
        conn = pool.getconn()

        # ... query logic ...

    except psycopg2.OperationalError as e:
        # Network-level connection failure
        if pool and conn:
            pool.putconn(conn, close=True)  # Force fresh connection

        if attempt < max_retries - 1:
            logger.warning(
                f"Database connection error fetching pending jobs (attempt {attempt + 1}/{max_retries}): {e}"
            )
            time.sleep(retry_delay)
            retry_delay *= 2  # Exponential backoff
        else:
            logger.error(f"Failed to fetch pending jobs after {max_retries} attempts")
            return []

    except Exception as e:
        # General exception handling
        if pool and conn:
            try:
                pool.putconn(conn)
            except:
                pass

        if attempt < max_retries - 1:
            logger.warning(f"Error fetching pending jobs (attempt {attempt + 1}/{max_retries}): {e}")
            time.sleep(retry_delay)
        else:
            logger.error(f"Error fetching pending jobs after {max_retries} attempts")
            return []
```

---

### 3. compute/core/engine.py

**Function**: `_clean_stale_offset()`
**Change**: Already implemented (verified in place)

**Purpose**: Delete offset file when replication slot is missing
**Called by**: `run()` method before building Debezium properties
**Behavior**:

1. Checks if offset file exists
2. Validates PostgreSQL replication slot
3. Deletes offset if slot missing or on validation error
4. Allows Debezium to start fresh from current WAL position

---

### 4. compute/core/manager.py

**Function**: `run_pipeline()` - subprocess initialization
**Status**: No changes needed (already optimized)

**Current configuration**:

```python
pipeline_pool_max_conn = int(os.getenv("PIPELINE_POOL_MAX_CONN", "10"))
init_connection_pool(min_conn=2, max_conn=pipeline_pool_max_conn)
```

---

### 5. Dockerfile

**Changes**: (from previous iteration)

- Python version: 3.12-slim-bookworm
- Removed uv.lock copying (fresh dependency resolution)
- Added `SNOWFLAKE_HOME=/tmp/.snowflake`
- Added `PIPELINE_POOL_MAX_CONN` environment variable

---

### 6. backend/app/core/config.py

**Changes**: (from previous iteration)

- wal_monitor_interval_seconds default: 30 → 60 seconds

---

### 7. pyproject.toml files

**Changes**: (from previous iteration)

- Python requirement: >=3.12 (was >=3.11)
- Location: `backend/pyproject.toml` and `compute/pyproject.toml`

---

## Summary of Changes

| Component             | Issue                    | Solution                                 | Impact                         |
| --------------------- | ------------------------ | ---------------------------------------- | ------------------------------ |
| Connection Pool Init  | Docker startup timing    | 5-attempt retry with exponential backoff | Eliminates startup failures    |
| Connection Validation | Dead connections in pool | `SELECT 1` health check                  | Prevents stale connections     |
| TCP Keepalive         | Connection drops         | keepalives settings enabled              | Detects network failures       |
| Statement Timeout     | Hanging queries          | 30-second timeout                        | Prevents connection stalling   |
| Pool Size             | Exhaustion under load    | 10 max per pipeline                      | Supports concurrent operations |
| Backfill Retry        | Network transients       | 3-attempt with backoff                   | Resilient job fetching         |
| Offset Cleanup        | LSN mismatch             | Auto-delete on slot change               | Clean restart capability       |

---

## Testing Checklist

- [ ] Docker build completes without errors
- [ ] Compute service starts successfully
- [ ] Connection pool initialization succeeds on first attempt
- [ ] No "server closed the connection unexpectedly" in logs
- [ ] Backfill jobs complete successfully
- [ ] Debezium replicates data correctly
- [ ] Replication slots are preserved after restart
- [ ] Offset files cleaned when slots are dropped

---

## Deployment Steps

1. **Build Docker image**:

   ```bash
   docker build -t rosetta:latest .
   ```

2. **Deploy to production**:

   ```bash
   docker-compose down
   docker-compose up -d
   ```

3. **Monitor logs**:

   ```bash
   docker-compose logs -f compute
   ```

4. **Validate initialization**:

   ```
   Look for: "Connection pool initialized successfully on attempt 1"
   ```

5. **Monitor for errors**:
   ```
   Should NOT see: "server closed the connection unexpectedly"
   ```

---

## Key Metrics to Monitor

### Connection Pool Health

- Pool utilization per pipeline
- Retries during initialization
- Dead connection removals

### Backfill Manager

- Pending job fetch success rate
- Retry counts per attempt
- OperationalError frequency

### Debezium Engine

- Offset cleanup instances
- Successful data replication
- CDC lag metrics

---

## Rollback Plan

If issues occur:

1. **Revert to previous Docker image**:

   ```bash
   docker-compose up -d --force-recreate
   ```

2. **Check logs for specific errors**:

   ```bash
   docker-compose logs compute | grep -i error
   ```

3. **Common troubleshooting**:
   - If "connection refused": Database not ready, use longer startup delay
   - If "operation timeout": Increase `statement_timeout` value
   - If "pool exhausted": Increase `PIPELINE_POOL_MAX_CONN` environment variable
