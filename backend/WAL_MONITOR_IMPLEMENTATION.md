# WAL Monitor Implementation - Upsert Pattern

## Overview

The WAL Monitor system tracks real-time Write-Ahead Log (WAL) replication status for PostgreSQL sources. It implements an **upsert pattern** where each source has exactly **one monitor record** that gets updated on each save operation.

## Key Features

✅ **1 Source = 1 Row**: Database constraint ensures uniqueness  
✅ **Upsert Pattern**: INSERT on first save, UPDATE on subsequent saves  
✅ **PostgreSQL ON CONFLICT**: Native database-level upsert support  
✅ **Real-time Status**: Track WAL LSN, position, lag, and replication status  
✅ **Error Tracking**: Capture and store error states  
✅ **RESTful API**: Complete CRUD operations with FastAPI

---

## Database Schema

### Table: `wal_monitor`

```sql
CREATE TABLE IF NOT EXISTS wal_monitor (
    id SERIAL PRIMARY KEY,
    source_id INTEGER NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    wal_lsn VARCHAR(255),           -- Log Sequence Number (e.g., '0/1A2B3C4D')
    wal_position BIGINT,            -- WAL position as numeric value
    last_wal_received TIMESTAMP,   -- Last time WAL data was received
    last_transaction_time TIMESTAMP, -- Last transaction timestamp
    replication_slot_name VARCHAR(255), -- Name of the replication slot
    replication_lag_bytes BIGINT,   -- Replication lag in bytes
    status VARCHAR(20) DEFAULT 'ACTIVE', -- 'ACTIVE', 'IDLE', 'ERROR'
    error_message TEXT,             -- Error details if any
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    CONSTRAINT unique_source_wal UNIQUE (source_id) -- Ensures 1 source = 1 row
);

-- Indexes for performance
CREATE INDEX idx_wal_monitor_source_id ON wal_monitor(source_id);
CREATE INDEX idx_wal_monitor_status ON wal_monitor(status);
CREATE INDEX idx_wal_monitor_last_received ON wal_monitor(last_wal_received);
```

### Key Constraint

The `CONSTRAINT unique_source_wal UNIQUE (source_id)` ensures that only **one row** exists per source. This enables the upsert pattern.

---

## Implementation Components

### 1. **Model** (`app/domain/models/wal_monitor.py`)

SQLAlchemy ORM model with:

- Unique constraint on `source_id`
- One-to-one relationship with Source
- Automatic timestamp management

```python
class WALMonitor(Base):
    __tablename__ = "wal_monitor"
    __table_args__ = (
        UniqueConstraint('source_id', name='unique_source_wal'),
        {"comment": "Real-time WAL replication status per source"}
    )
    # ... fields ...
```

### 2. **Repository** (`app/domain/repositories/wal_monitor_repo.py`)

Implements the upsert pattern using PostgreSQL's `INSERT ... ON CONFLICT ... DO UPDATE`:

```python
async def upsert_monitor(self, source_id: int, **kwargs) -> WALMonitor:
    """Insert or update WAL monitor record."""
    stmt = insert(WALMonitor).values(
        source_id=source_id,
        # ... other fields ...
    )

    # ON CONFLICT: Update existing row instead of failing
    stmt = stmt.on_conflict_do_update(
        constraint='unique_source_wal',
        set_={
            'wal_lsn': stmt.excluded.wal_lsn,
            'wal_position': stmt.excluded.wal_position,
            # ... update all fields ...
            'updated_at': now,
        }
    ).returning(WALMonitor)

    result = await self.db.execute(stmt)
    return result.scalar_one()
```

#### Key Repository Methods

| Method               | Description                                    |
| -------------------- | ---------------------------------------------- |
| `upsert_monitor()`   | Insert new or update existing monitor (UPSERT) |
| `get_by_source()`    | Retrieve monitor by source_id                  |
| `get_all_monitors()` | List all monitor records                       |
| `update_status()`    | Quick status update                            |
| `delete_by_source()` | Remove monitor record                          |

### 3. **Service** (`app/domain/services/wal_monitor_service.py`)

Business logic layer:

- Validates source exists before upsert
- Handles transactions
- Provides error handling

### 4. **API Endpoints** (`app/api/v1/endpoints/wal_monitor.py`)

RESTful API with FastAPI:

| Endpoint                                  | Method | Description           |
| ----------------------------------------- | ------ | --------------------- |
| `/wal-monitor/sources/{source_id}`        | POST   | Upsert monitor record |
| `/wal-monitor/sources/{source_id}`        | GET    | Get monitor by source |
| `/wal-monitor/`                           | GET    | List all monitors     |
| `/wal-monitor/sources/{source_id}/status` | PATCH  | Update status only    |
| `/wal-monitor/sources/{source_id}`        | DELETE | Delete monitor        |

---

## Usage Examples

### 1. First Save (INSERT)

```python
import httpx

async with httpx.AsyncClient() as client:
    data = {
        "source_id": 1,
        "wal_lsn": "0/1A2B3C4D",
        "wal_position": 440401997,
        "status": "ACTIVE",
    }

    # First call creates a new record
    response = await client.post(
        "http://localhost:8000/api/v1/wal-monitor/sources/1",
        json=data
    )

    monitor = response.json()
    print(f"Created monitor ID: {monitor['id']}")  # e.g., 1
```

### 2. Second Save (UPDATE)

```python
# Update with new WAL position
data = {
    "source_id": 1,
    "wal_lsn": "0/1A2B3C5E",  # Advanced position
    "wal_position": 440402014,
    "status": "ACTIVE",
}

# Same endpoint, but updates existing record
response = await client.post(
    "http://localhost:8000/api/v1/wal-monitor/sources/1",
    json=data
)

monitor = response.json()
print(f"Same monitor ID: {monitor['id']}")  # Still 1, not 2!
print(f"Updated LSN: {monitor['wal_lsn']}")  # 0/1A2B3C5E
```

### 3. Quick Status Update

```python
# Update only status field
status_data = {
    "status": "ERROR",
    "error_message": "Connection timeout"
}

response = await client.patch(
    "http://localhost:8000/api/v1/wal-monitor/sources/1/status",
    json=status_data
)
```

### 4. Query Current State

```python
# Get current monitor state
response = await client.get(
    "http://localhost:8000/api/v1/wal-monitor/sources/1"
)

monitor = response.json()
print(f"Current status: {monitor['status']}")
print(f"Current LSN: {monitor['wal_lsn']}")
print(f"Lag: {monitor['replication_lag_bytes']} bytes")
```

---

## How Upsert Works

### Database Level (PostgreSQL)

```sql
-- First call (source_id=1 doesn't exist)
INSERT INTO wal_monitor (source_id, wal_lsn, ...)
VALUES (1, '0/1A2B3C4D', ...)
ON CONFLICT (source_id) DO UPDATE SET ...
-- Result: INSERT (creates new row)

-- Second call (source_id=1 exists)
INSERT INTO wal_monitor (source_id, wal_lsn, ...)
VALUES (1, '0/1A2B3C5E', ...)
ON CONFLICT (source_id) DO UPDATE SET
    wal_lsn = EXCLUDED.wal_lsn,
    wal_position = EXCLUDED.wal_position,
    updated_at = NOW()
-- Result: UPDATE (modifies existing row)
```

### Application Level (SQLAlchemy)

```python
from sqlalchemy.dialects.postgresql import insert

stmt = insert(WALMonitor).values(
    source_id=1,
    wal_lsn="0/1A2B3C5E",
)

stmt = stmt.on_conflict_do_update(
    constraint='unique_source_wal',  # The UNIQUE constraint name
    set_={
        'wal_lsn': stmt.excluded.wal_lsn,  # New value
        'updated_at': datetime.utcnow(),
    }
).returning(WALMonitor)

result = await db.execute(stmt)
monitor = result.scalar_one()  # Returns inserted or updated row
```

---

## Comparison: WAL Metric vs WAL Monitor

| Feature             | WAL Metric             | WAL Monitor          |
| ------------------- | ---------------------- | -------------------- |
| **Purpose**         | Historical tracking    | Current status       |
| **Pattern**         | Append-only            | Upsert               |
| **Rows per source** | Many (time-series)     | One (latest state)   |
| **Use case**        | Trend analysis, charts | Real-time monitoring |
| **Table**           | `wal_metrics`          | `wal_monitor`        |
| **Constraint**      | None                   | `UNIQUE(source_id)`  |

### When to Use Each

- **WAL Metric**: Historical data, time-series analysis, graphs over time
- **WAL Monitor**: Current replication status, dashboard, alerting

---

## Migration

### Alembic Migration

Run the migration to create the table:

```bash
# From backend/ directory
alembic upgrade head
```

Migration file: `alembic/versions/002_add_wal_monitor.py`

### Manual SQL

If not using Alembic, run the SQL from:

- `migrations/001_create_table.sql` (already updated)

---

## Testing

### Run the Example

```bash
cd backend
python examples/wal_monitor_upsert_example.py
```

This demonstrates:

1. First upsert (INSERT)
2. Second upsert (UPDATE with same ID)
3. Status updates
4. Multiple sources
5. Querying and deletion

### Expected Behavior

```
1st call: Creates monitor ID=1 for source_id=1
2nd call: Updates monitor ID=1 (same ID!) for source_id=1
3rd call: Updates monitor ID=1 (still same ID!)
```

---

## Benefits of Upsert Pattern

✅ **No Duplicate Rows**: Database enforces uniqueness  
✅ **Automatic**: No need to check if exists before insert/update  
✅ **Atomic**: Single database operation, no race conditions  
✅ **Efficient**: One query instead of SELECT + INSERT/UPDATE  
✅ **Clean API**: Single endpoint for both create and update  
✅ **Simple Code**: Repository handles complexity, service is clean

---

## Error Handling

### Unique Constraint Violation

The `UNIQUE` constraint prevents duplicate inserts. The `ON CONFLICT` clause handles this gracefully by updating instead.

### Source Not Found

The service validates that the source exists before upserting:

```python
# Raises EntityNotFoundError if source doesn't exist
source = await self.source_repo.get_by_id(source_id)
if not source:
    raise EntityNotFoundError(entity_type="Source", entity_id=source_id)
```

---

## Performance Considerations

### Indexes

Three indexes optimize common queries:

1. **`idx_wal_monitor_source_id`**: Fast lookups by source
2. **`idx_wal_monitor_status`**: Filter by status (ACTIVE, ERROR, etc.)
3. **`idx_wal_monitor_last_received`**: Time-based queries

### Concurrent Updates

The upsert operation is atomic at the database level. Multiple concurrent upserts for the same source_id will:

- Serialize (queue) at the database level
- Last write wins
- No lost updates or race conditions

---

## Monitoring & Observability

### Logging

All operations log structured data:

```python
logger.info(
    "WAL monitor upserted",
    extra={
        "source_id": source_id,
        "monitor_id": monitor.id,
        "status": status,
    }
)
```

### Metrics to Track

- Upsert frequency per source
- Status distribution (ACTIVE vs ERROR)
- Replication lag trends
- Error rates

---

## Future Enhancements

### Potential Additions

1. **Timestamp Tracking**: Add `last_checked_at` for monitoring health
2. **Retention Policy**: Auto-delete stale monitors (sources not checked in X days)
3. **Alerting**: Trigger alerts when status changes to ERROR
4. **Batch Upsert**: Accept array of monitors for bulk operations
5. **Change History**: Separate audit table for status changes

### Alternative Patterns

If you need history of changes, consider:

- Keeping `wal_monitor` for current state (upsert)
- Adding `wal_monitor_history` for audit trail (append-only)

---

## Summary

The WAL Monitor implementation provides a robust, efficient solution for tracking real-time replication status:

- ✅ Database-enforced uniqueness via `UNIQUE` constraint
- ✅ Efficient upsert via PostgreSQL's `ON CONFLICT`
- ✅ Clean API with single endpoint for create/update
- ✅ Atomic operations with no race conditions
- ✅ Full CRUD support with FastAPI
- ✅ Type-safe with Pydantic schemas
- ✅ Tested with example scripts

**Key Takeaway**: The upsert pattern ensures each source maintains exactly one monitor record, automatically inserting on first save and updating on subsequent saves, all handled transparently at the database level.
