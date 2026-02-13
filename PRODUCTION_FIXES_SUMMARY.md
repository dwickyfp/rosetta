# Rosetta Production Fixes Summary

## Issue: "server closed the connection unexpectedly"

### Root Cause

PostgreSQL connections dropped unexpectedly in production Docker environment due to:

1. **Docker startup timing**: Config database (PostgreSQL) not immediately available when compute service initializes
2. **Stale connections**: Dead connections not detected/removed from pool
3. **Transient network failures**: Brief connectivity interruptions in containerized environment
4. **Connection exhaustion**: Pool size insufficient for concurrent operations

### Solutions Implemented

#### 1. Connection Pool Initialization Retry Logic

**File**: `compute/core/database.py` - `init_connection_pool()`

**Implementation**:

- **Retry attempts**: 5 attempts to establish connection pool
- **Backoff strategy**: Exponential backoff (2s → 3s → 4.5s → 6.75s → 10s)
- **Total wait time**: Up to ~27 seconds for all retries
- **Logging**: WARNING log for each attempt, ERROR log on final failure

**Code**:

```python
for attempt in range(max_retries):
    try:
        _connection_pool = pool.ThreadedConnectionPool(...)
        logger.info(f"Connection pool initialized successfully on attempt {attempt + 1}")
        return _connection_pool
    except psycopg2.Error as e:
        if attempt < max_retries - 1:
            time.sleep(retry_delay)
            retry_delay *= 1.5  # Exponential backoff
```

**Impact**: Eliminates immediate startup failures when PostgreSQL is not yet available

#### 2. Connection Health Validation

**File**: `compute/core/database.py` - `get_db_connection()`

**Implementation**:

- **Health check**: Execute `SELECT 1` query on retrieved connection
- **Dead connection detection**: Catch `psycopg2.OperationalError` and `psycopg2.InterfaceError`
- **Auto-removal**: Dead connections removed from pool with `pool.putconn(conn, close=True)`
- **Fresh retry**: Automatically get a new connection if one fails

**Code**:

```python
conn = pool.getconn()
try:
    with conn.cursor() as cur:
        cur.execute("SELECT 1")  # Health check
except (psycopg2.OperationalError, psycopg2.InterfaceError):
    pool.putconn(conn, close=True)  # Remove dead connection
    conn = pool.getconn()  # Get fresh one
```

**Impact**: Prevents dead connections from being returned to calling code

#### 3. Connection Keepalive Settings

**File**: `compute/core/database.py` - `init_connection_pool()`

**TCP Keepalive Configuration**:

```python
"keepalives": 1,               # Enable TCP keepalive
"keepalives_idle": 30,         # Start keepalive after 30s idle
"keepalives_interval": 10,     # Keepalive probe every 10s
"keepalives_count": 5          # 5 lost probes = connection closed
```

**Impact**: Detects and removes dead connections due to network failures

#### 4. Statement Timeout

**File**: `compute/core/database.py` - `init_connection_pool()`

**Configuration**:

```python
"options": "-c statement_timeout=30000"  # 30 seconds
```

**Purpose**: Prevent queries from holding connections indefinitely if they hang

#### 5. Connection Pool Size Optimization

**File**: `compute/core/manager.py` - subprocess initialization

**Per-pipeline allocation**:

- **min_conn**: 2 (minimum to maintain)
- **max_conn**: 10 (configurable via `PIPELINE_POOL_MAX_CONN` env var)

**Breakdown**:

- Main engine queries: 2-3 connections
- Backfill manager: 3-4 connections
- DLQ recovery: 1-2 connections
- Buffer for spikes: 2 connections

#### 6. Backfill Manager Connection Error Handling

**File**: `compute/core/backfill_manager.py` - `_get_pending_jobs()`

**Improvements**:

- **Separate exception handling**: `psycopg2.OperationalError` (network failures) vs general exceptions
- **Retry logic**: 3 attempts with exponential backoff (2s → 4s → 8s)
- **Proper cleanup**: `pool.putconn(conn, close=True)` on OperationalError
- **Logging**: Detailed logs per attempt with retry counts

**Code Example**:

```python
except psycopg2.OperationalError as e:
    pool.putconn(conn, close=True)  # Force fresh connection
    retry_delay *= 2  # Exponential backoff
    # Retry logic...
```

#### 7. Stale Offset File Cleanup

**File**: `compute/core/engine.py` - `_clean_stale_offset()`

**Behavior**:

1. Validates PostgreSQL replication slot exists
2. If slot missing: deletes offset file (prevents LSN mismatch)
3. On validation error: deletes offset as safety measure
4. Called BEFORE Debezium engine starts

**Problem solved**:

- Prevents "LSN no longer available on server" errors
- Allows clean restart when slots are recreated

#### 8. Python Version & Dependencies

**File**: `Dockerfile` & `pyproject.toml`

**Changes**:

- **Python version**: 3.12-slim-bookworm (upgraded from 3.11)
- **uv.lock**: Bypassed for fresh dependency resolution
- **JPype**: Improved GIL handling in Python 3.12

#### 9. Configuration Defaults

**File**: `backend/app/core/config.py`

**Changes**:

- **wal_monitor_interval_seconds**: Changed default from 30 → 60 seconds
- **Reason**: Minimum validation requirement was `ge=60`

---

## Docker Environment Configuration

### Environment Variables

```yaml
PIPELINE_POOL_MAX_CONN: "10" # Max connections per pipeline
CONFIG_DATABASE_URL: "..." # Config DB connection string
DEBUG: "false"
LOG_LEVEL: "INFO"
REDIS_URL: "redis://redis:6379"
SNOWFLAKE_HOME: "/tmp/.snowflake" # Writable tmp directory
```

### Important Notes

- `tmp/offsets/` directory is NOT a Docker volume (ephemeral)
- Offset files are cleaned on container restart (expected behavior)
- Replication slots preserved in PostgreSQL (via `slot.drop.on.stop: false`)

---

## Deployment Checklist

- [ ] Build Docker image with updated `database.py` and `manager.py`
- [ ] Deploy compute service with new connection retry logic
- [ ] Monitor logs for "Connection pool initialized successfully on attempt X"
- [ ] Verify no "server closed the connection unexpectedly" errors
- [ ] Check for "Deleting stale offset" messages (healthy behavior)
- [ ] Monitor database connection pool metrics
- [ ] Verify backfill jobs complete successfully (3 retries with backoff)

---

## Monitoring & Validation

### Success Indicators

1. **Connection pool initialization**: Single attempt (no retries needed)

   ```
   Connection pool initialized successfully on attempt 1
   ```

2. **Healthy backfill operations**: No "server closed connection" errors

   ```
   Successfully fetched X pending jobs
   ```

3. **Offset management**: Appropriate cleanup on slot changes
   ```
   Deleted stale offset. Debezium will start fresh.
   ```

### Failure Indicators

1. **Multiple retry attempts**: Suggests Docker startup timing issues

   ```
   Failed to initialize connection pool (attempt 1/5): Connection refused
   Retrying in 2.0s...
   ```

2. **OperationalError exceptions**: Network-level connection failures

   ```
   Database connection error fetching pending jobs: server closed the connection unexpectedly
   ```

3. **Offset validation errors**: Replication slot issues
   ```
   Could not validate offset: relation does not exist
   ```

---

## Key Parameters & Tuning

### Connection Timeouts

- **connect_timeout**: 10 seconds (initial connection)
- **statement_timeout**: 30 seconds (per query)
- **keepalives_idle**: 30 seconds (before TCP keepalive probe)

### Retry Strategy

- **Init pool**: 5 attempts, exponential backoff (2-10s)
- **Backfill jobs**: 3 attempts, exponential backoff (2-8s)
- **Engine queries**: Handled by statement timeout

### Pool Sizes

- **Min connections**: 2 per pipeline
- **Max connections**: 10 per pipeline (configurable)

---

## Testing Recommendations

### Docker Startup Timing

```bash
# Test with PostgreSQL startup delay
docker-compose up -d postgres
sleep 5  # Delay before compute service starts
docker-compose up compute
```

### Connection Failure Simulation

```bash
# Kill connection to config database
docker-compose exec postgres killall -9 postgres
# Should see retry logs, recover without crashing
```

### Offset Cleanup Validation

```bash
# Drop replication slot manually
docker-compose exec source_db psql -U replicator -d source_db -c \
  "SELECT pg_drop_replication_slot('debezium_slot');"
# Restart pipeline
# Should see "Deleted stale offset" message
```

---

## Related Issues Fixed

1. ✅ GIL threading error (Python 3.11 → 3.12)
2. ✅ Circular import in configuration (removed DB load from init)
3. ✅ Connection pool exhaustion (increased pool sizes)
4. ✅ Stale offset LSN mismatch (auto-cleanup mechanism)
5. ✅ Snowflake permission errors (SNOWFLAKE_HOME env var)
6. ✅ Docker connection failures (retry + keepalive logic)

---

## Version References

- **Python**: 3.12-slim-bookworm
- **JPype**: 1.6.0+ (improved GIL handling)
- **Debezium**: 3.4.1 (pgoutput plugin)
- **psycopg2**: Latest (ThreadedConnectionPool with keepalive)
- **PostgreSQL**: 12+ (logical replication support)
