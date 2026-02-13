# Production Deployment Verification Checklist

## Pre-Deployment Verification ✅

### Code Changes Applied

- [x] `compute/core/database.py`: Retry logic with exponential backoff added (5 attempts, 2-10s)
- [x] `compute/core/database.py`: Connection health check with `SELECT 1` query
- [x] `compute/core/database.py`: TCP keepalive settings configured
- [x] `compute/core/database.py`: Statement timeout set to 30 seconds
- [x] `compute/core/backfill_manager.py`: OperationalError handling with separate retry logic
- [x] `compute/core/engine.py`: Stale offset cleanup mechanism verified
- [x] `compute/core/manager.py`: Connection pool size configuration verified
- [x] `Dockerfile`: Python 3.12 verified
- [x] `backend/app/core/config.py`: wal_monitor_interval_seconds default fixed

### All Imports Verified

- [x] `import time` in `database.py` (line 8)
- [x] `psycopg2.OperationalError` available in backfill_manager
- [x] `pool.ThreadedConnectionPool` available from psycopg2

### Test Cases to Validate

#### 1. Docker Startup Timing

**Scenario**: PostgreSQL not immediately available when compute starts
**Expected**: Connection pool initialization retries and succeeds
**Log signature**:

```
Failed to initialize connection pool (attempt 1/5): Connection refused. Retrying in 2.0s...
Connection pool initialized successfully on attempt 2
```

#### 2. Dead Connection Detection

**Scenario**: Database connection dies mid-pipeline
**Expected**: Dead connection detected, removed, fresh connection obtained
**Log signature**:

```
Detected dead connection, removing from pool
```

#### 3. Backfill Job Retry

**Scenario**: Network interruption while fetching pending jobs
**Expected**: Retry with exponential backoff
**Log signature**:

```
Database connection error fetching pending jobs (attempt 1/3): server closed the connection unexpectedly
Retrying in 2s...
Database connection error fetching pending jobs (attempt 2/3): server closed the connection unexpectedly
Retrying in 4s...
Successfully fetched 0 pending jobs
```

#### 4. Stale Offset Cleanup

**Scenario**: Replication slot manually dropped, pipeline restarted
**Expected**: Offset file deleted, Debezium starts fresh
**Log signature**:

```
Deleting stale offset file: tmp/offsets/pipeline_name.dat
Deleted stale offset. Debezium will start fresh with snapshot mode.
```

---

## Deployment Instructions

### Step 1: Build Docker Image

```bash
cd d:\Research\rosetta
docker build -t rosetta:v1-prod .
```

**Verification**:

- Build completes without errors
- Final image size ~800-900MB (Python 3.12 slim)

### Step 2: Deploy Container

```bash
docker-compose down
docker-compose up -d
```

**Wait for startup**: ~30 seconds for services to stabilize

### Step 3: Monitor Initial Logs

```bash
docker-compose logs -f compute --tail 50
```

**Look for**:
✅ `Connection pool initialized successfully on attempt 1`
✅ `Pipeline manager started with N active pipelines`
✅ No ERROR level logs during startup

**Red flags**:
❌ `Failed to initialize connection pool after 5 attempts`
❌ `server closed the connection unexpectedly` (immediate startup)

### Step 4: Validate Pipelines

```bash
# Check if pipelines are running
curl http://localhost:8000/api/v1/pipelines

# Expected response: List of pipelines with status=START/PAUSE
```

### Step 5: Monitor Backfill Operations

```bash
# If backfill jobs exist
docker-compose logs compute | grep -E "(backfill|pending jobs|fetch)"

# Expected pattern:
# Successfully fetched X pending jobs
# OR: No pending backfill jobs
```

### Step 6: Test Data Replication

```bash
# Insert test record in source database
docker-compose exec source_db psql -U replicator -d source_db -c \
  "INSERT INTO test_table VALUES (1, 'test data', NOW());"

# Check if replicated to destination
docker-compose exec postgres psql -U replicator -d config_db -c \
  "SELECT * FROM test_table WHERE id = 1;"
```

---

## Success Criteria

### Immediate (First 5 minutes)

- [ ] Compute service starts without errors
- [ ] Connection pool initializes successfully
- [ ] No "server closed connection" errors in startup logs

### Short-term (First hour)

- [ ] Pipelines running and replicating data
- [ ] Backfill jobs (if any) progressing without errors
- [ ] No unhandled exceptions in logs
- [ ] Database connection pool stable (not exhausted)

### Long-term (24+ hours)

- [ ] Zero "server closed the connection unexpectedly" errors
- [ ] Pipeline lag metrics stable
- [ ] Offset cleanup occurring appropriately (when slots change)
- [ ] All retry mechanisms functioning as expected

---

## Rollback Procedures

### If Connection Issues Occur

```bash
# Increase pool size
docker-compose down
docker-compose up -d -e PIPELINE_POOL_MAX_CONN=15
```

### If Timeout Issues Occur

Edit `compute/core/database.py` and increase:

```python
"statement_timeout": 60000  # Change from 30000 to 60000
```

### If Database Connection Fails

Check PostgreSQL health:

```bash
docker-compose logs postgres | tail -20
docker-compose exec postgres pg_isready
```

### Full Rollback to Previous Version

```bash
docker-compose down
# Remove volumes if needed
docker volume rm rosetta_postgres_data  # WARNING: Deletes data!
# Checkout previous code version
git checkout <previous-commit>
docker build -t rosetta:v0 .
docker-compose up -d
```

---

## Monitoring & Alerting Setup

### Key Metrics to Monitor

#### 1. Connection Pool Utilization

```sql
-- Check active connections
SELECT count(*) FROM pg_stat_activity
WHERE datname = 'config_db';
```

Target: < 8 connections per pipeline (max=10)

#### 2. Log Error Rate

```bash
# Count "server closed connection" errors
docker-compose logs compute | grep -i "closed" | wc -l
```

Target: 0 after stabilization

#### 3. Backfill Success Rate

```bash
# Count successful job fetches
docker-compose logs compute | grep "fetched.*jobs" | tail -10
```

Expected: Regular successful fetches with no gaps

#### 4. Debezium Lag

```bash
# Check WAL monitor output
docker-compose logs compute | grep "wal_size" | tail -5
```

Expected: Stable or decreasing WAL size

### Alert Thresholds

| Metric                     | Warning    | Critical     | Action                       |
| -------------------------- | ---------- | ------------ | ---------------------------- |
| Connection pool size       | >8 active  | >9 active    | Check query performance      |
| "Closed connection" errors | 1 per hour | 1 per minute | Restart compute service      |
| Backfill retry attempts    | >1 per job | >2 per job   | Check database health        |
| Debezium WAL lag           | >100MB     | >500MB       | Check destination throughput |

---

## Performance Baselines

### Expected Startup Time

- Container startup: ~10s
- PostgreSQL readiness: ~15s
- Connection pool init: 1-3s (with retries if needed)
- Total: ~30s to first pipeline startup

### Expected Query Performance

- Connection health check (`SELECT 1`): <1ms
- Pending job query: <100ms
- Pool.getconn(): <100ms

### Expected Memory Usage

- Per pipeline subprocess: ~100-150MB
- Backfill batch (10k rows): ~50-75MB

---

## Troubleshooting Guide

### Issue: "server closed the connection unexpectedly" appears in logs

**Diagnosis**:

1. Check PostgreSQL logs: `docker-compose logs postgres | grep -i error`
2. Check network connectivity: `docker exec rosetta-compute ping postgres`
3. Check statement timeout: Look for long-running queries

**Solutions**:

1. Increase statement timeout: `"options": "-c statement_timeout=60000"`
2. Check PostgreSQL resource limits
3. Increase pool size: `PIPELINE_POOL_MAX_CONN=15`

### Issue: Connection pool initialization fails all 5 attempts

**Diagnosis**:

1. Check PostgreSQL is running: `docker-compose ps postgres`
2. Check credentials in CONFIG_DATABASE_URL
3. Check firewall/network access

**Solutions**:

1. Wait longer for PostgreSQL to start: `sleep 30` before compute start
2. Verify connection string format
3. Check PostgreSQL logs for authentication errors

### Issue: Backfill jobs stuck or failing repeatedly

**Diagnosis**:

1. Check backfill logs: `docker-compose logs compute | grep backfill`
2. Check connection pool exhaustion
3. Check for deadlocks: `docker-compose exec postgres pg_stat_statements`

**Solutions**:

1. Increase pool size
2. Check for long-running queries blocking the pool
3. Restart compute service

---

## Post-Deployment Validation

Run this script after 1 hour of operation:

```bash
#!/bin/bash
echo "=== Connection Pool Health ==="
docker-compose logs compute | grep "Connection pool" | tail -1

echo -e "\n=== Error Count (last hour) ==="
docker-compose logs compute | grep -i "error\|exception" | wc -l

echo -e "\n=== 'Closed Connection' Errors ==="
docker-compose logs compute | grep -i "closed" | wc -l

echo -e "\n=== Active Pipelines ==="
curl -s http://localhost:8000/api/v1/pipelines | jq '.[] | {id, status}' | head -10

echo -e "\n=== Database Connections ==="
docker-compose exec postgres psql -U replicator -d config_db \
  -c "SELECT count(*) as active_connections FROM pg_stat_activity WHERE datname='config_db';"

echo -e "\n=== Last successful backfill check ==="
docker-compose logs compute | grep "fetched.*jobs" | tail -1

echo -e "\n=== Offset cleanup history ==="
docker-compose logs compute | grep -i "offset" | tail -3
```

**Expected output**:

- Connection pool initialized successfully: ✅
- Error count: 0-2 (minor warnings acceptable)
- Closed connection errors: 0
- Active pipelines: >0
- Database connections: <10
- Last backfill: Recent (within last 5 minutes)
- Offset cleanup: Shows appropriate deletions

---

## Sign-Off

**Deployment Date**: **********\_**********
**Deployed By**: **********\_**********
**Reviewed By**: **********\_**********

**Pre-deployment checks completed**: [ ]
**All tests passed**: [ ]
**Monitoring alerts active**: [ ]
**Rollback plan validated**: [ ]
