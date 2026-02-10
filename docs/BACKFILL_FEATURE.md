# Backfill Data Feature

## Overview

The Backfill Data feature allows you to retroactively synchronize historical data from your source PostgreSQL database to destination systems (e.g., Snowflake) using DuckDB for efficient batch processing.

## Architecture

### Components

1. **Backend API** (`backend/app/`)
   - Models: `domain/models/queue_backfill.py`
   - Schemas: `domain/schemas/backfill.py`
   - Repository: `domain/repositories/backfill.py`
   - Service: `domain/services/backfill.py`
   - Endpoints: `api/v1/endpoints/backfill.py`

2. **Compute Engine** (`compute/`)
   - Backfill Manager: `core/backfill_manager.py`
   - Integrated into: `main.py`

3. **Frontend** (`web/src/`)
   - API Client: `repo/backfill.ts`
   - Components: `features/pipelines/components/backfill-data-tab.tsx`
   - Integration: `features/pipelines/pages/pipeline-details-page.tsx`

### Flow

```
1. User creates backfill job via UI
   └─> POST /pipelines/{id}/backfill

2. Job saved to queue_backfill_data table
   └─> status: PENDING

3. Compute BackfillManager polls every 5 seconds
   └─> Finds PENDING jobs

4. Job execution starts
   ├─> status: EXECUTING
   ├─> DuckDB attaches to PostgreSQL
   ├─> SELECT with optional WHERE filters
   ├─> Process in batches (10,000 rows)
   └─> Send to destination handlers

5. Job completes
   ├─> status: COMPLETED/FAILED
   └─> count_record updated
```

## Features

### Filter Support

- **Maximum 5 filters** per backfill job
- Filters are semicolon-separated in database
- Supported operators:
  - `=` (Equals)
  - `!=` (Not Equals)
  - `>` (Greater Than)
  - `<` (Less Than)
  - `>=` (Greater or Equal)
  - `<=` (Less or Equal)
  - `LIKE` (Pattern matching)
  - `ILIKE` (Case-insensitive pattern matching)
  - `IS NULL` (Null check)
  - `IS NOT NULL` (Not null check)

### Status Lifecycle

1. **PENDING**: Job created, waiting to be processed
2. **EXECUTING**: Currently running
3. **COMPLETED**: Successfully finished
4. **FAILED**: Encountered an error
5. **CANCELLED**: User-cancelled during execution

### Cancellation

- Users can cancel jobs in PENDING or EXECUTING status
- Cancellation is graceful - current batch completes
- Status updates to CANCELLED

### Memory Safety

- **Batch Processing**: Processes 10,000 rows at a time (configurable)
- **Threading**: Each job runs in a separate thread
- **Resource Limits**: Maximum concurrent jobs managed by thread pool
- **Connection Pooling**: Uses existing database connection pool

## API Endpoints

### Create Backfill Job

```http
POST /api/v1/pipelines/{pipeline_id}/backfill
```

**Request Body:**

```json
{
  "table_name": "users",
  "filters": [
    {
      "column": "created_at",
      "operator": ">=",
      "value": "2024-01-01"
    },
    {
      "column": "status",
      "operator": "=",
      "value": "active"
    }
  ]
}
```

### List Backfill Jobs

```http
GET /api/v1/pipelines/{pipeline_id}/backfill?skip=0&limit=100
```

### Get Backfill Job

```http
GET /api/v1/backfill/{job_id}
```

### Cancel Backfill Job

```http
POST /api/v1/backfill/{job_id}/cancel
```

### Delete Backfill Job

```http
DELETE /api/v1/backfill/{job_id}
```

## Database Schema

```sql
CREATE TABLE queue_backfill_data (
    id SERIAL PRIMARY KEY,
    pipeline_id INTEGER NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    source_id INTEGER NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    table_name VARCHAR(255) NOT NULL,
    filter_sql TEXT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    count_record BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
```

## Configuration

### Compute Settings

In `compute/main.py`:

```python
backfill_manager = BackfillManager(
    check_interval=5,    # Poll every 5 seconds
    batch_size=10000     # 10K rows per batch
)
```

### Environment Variables

No additional environment variables required. Uses existing:

- `CONFIG_DATABASE_URL`: Config database connection

## Usage

### From UI

1. Navigate to Pipeline Details page
2. Click "Backfill Data" tab
3. Click "Create Backfill" button
4. Select table name from dropdown
5. (Optional) Add up to 5 filters
6. Click "Create Job"
7. Monitor progress in the table
8. Cancel or delete jobs as needed

### From API

```python
import requests

# Create backfill job
response = requests.post(
    'http://localhost:8000/api/v1/pipelines/1/backfill',
    json={
        'table_name': 'orders',
        'filters': [
            {'column': 'order_date', 'operator': '>=', 'value': '2024-01-01'}
        ]
    }
)
job = response.json()

# Check status
status = requests.get(f'http://localhost:8000/api/v1/backfill/{job["id"]}')
print(status.json())
```

## Performance Considerations

### Batch Size

- Default: 10,000 rows
- Adjust based on:
  - Row size (wider tables = smaller batches)
  - Network latency
  - Destination write performance

### Concurrent Jobs

- Multiple jobs run in parallel threads
- Each job has independent DuckDB connection
- Monitor system resources

### DuckDB Memory

- In-memory database (`:memory:`)
- Memory scales with batch size
- For very large tables, batching is critical

## Troubleshooting

### DuckDB Not Installed

```bash
# Install DuckDB
pip install duckdb
```

### Job Stuck in EXECUTING

1. Check compute service logs
2. Check if DuckDB can connect to source
3. Verify PostgreSQL credentials
4. Cancel job and retry

### Slow Performance

1. Reduce batch size
2. Add more specific filters
3. Check network latency
4. Monitor PostgreSQL load

### Failed Jobs

1. Check error logs in compute service
2. Common issues:
   - Invalid table name
   - Invalid filter syntax
   - Connection timeout
   - Destination write failure

## Best Practices

1. **Start Small**: Test with filtered data before full table backfill
2. **Off-Peak Hours**: Run large backfills during low-traffic periods
3. **Monitor Progress**: Use the UI to track record counts
4. **Batch Appropriately**: Adjust batch size based on row width
5. **Clean Up**: Delete completed/failed jobs periodically

## Limitations

1. **Maximum 5 filters** per job
2. **No resume support** - cancelled jobs must be restarted
3. **No incremental backfill** - always processes entire result set
4. **Simple SQL filters** - complex joins not supported

## Future Enhancements

- [ ] Resume capability for cancelled jobs
- [ ] Incremental backfill (checkpoint support)
- [ ] Advanced SQL support (joins, subqueries)
- [ ] Scheduled backfills
- [ ] Email notifications on completion
- [ ] Progress percentage estimation
- [ ] Parallel batch processing
- [ ] Destination-specific optimizations

## Dependencies

- **DuckDB**: >= 0.10.0
- **psycopg2-binary**: >= 2.9.9
- PostgreSQL with logical replication enabled

## Related Features

- [CDC Replication](../docs/cdc-replication.md)
- [Pipeline Management](../docs/pipelines.md)
- [Destination Configuration](../docs/destinations.md)
