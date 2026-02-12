# Rosetta ETL Platform - AI Agent Instructions

## Project Overview

Rosetta is a real-time ETL platform with a **three-service architecture**:

- **Backend** (FastAPI/Python): Configuration API managing sources, destinations, and pipelines
- **Compute** (Python/Debezium): CDC engine executing data replication from PostgreSQL → Snowflake
- **Web** (React/TypeScript/Vite): Admin dashboard for pipeline management

## Architecture Patterns

### Backend: Clean Architecture with DDD

The backend follows strict Clean Architecture with clear layer separation:

```
api/v1/endpoints/          # Presentation: FastAPI routes
domain/services/           # Application: Business logic orchestration
domain/models/             # Domain: SQLAlchemy ORM entities
domain/schemas/            # Domain: Pydantic validation
domain/repositories/       # Infrastructure: Data access (Repository Pattern)
infrastructure/tasks/      # Infrastructure: Background tasks
```

**Critical conventions:**

- Services coordinate business logic and call repositories - never access ORM directly in endpoints
- Always use `PipelineService`, `SourceService`, etc. from `app.api.deps` dependency injection
- Repositories extend `BaseRepository` with generic CRUD operations
- Models use SQLAlchemy 2.0 async patterns with `AsyncSession`
- Status changes (START/PAUSE) must update both `Pipeline.status` and `PipelineMetadata.status`

### Web: Feature-Based Organization

Frontend uses feature-based structure with TanStack ecosystem:

```
features/<feature>/
  ├── components/          # Feature-specific UI components
  ├── pages/              # Route pages
  └── data/               # Zod schemas & table configs
```

**Critical conventions:**

- API calls in `src/repo/` files (axios client), never inline
- Use `useQuery` for reads, `useMutation` for writes with `@tanstack/react-query`
- After mutations, invalidate queries: `queryClient.invalidateQueries({ queryKey: ['destinations'] })`
- Add 300ms delay before invalidating to allow DB transactions to commit
- Table components use `@tanstack/react-table` with shadcn/ui patterns
- Forms use `react-hook-form` + `zod` validation with `@hookform/resolvers`

### Compute: Event-Driven CDC Engine

Compute service runs Debezium-based CDC pipelines with process isolation:

- Polls config database every **10 seconds** for `status='START'` pipelines
- Uses `pydbzengine` for PostgreSQL WAL replication
- Each pipeline runs in isolated `multiprocessing.Process` via `PipelineManager`
- Process crash isolation - one pipeline failure doesn't affect others
- Backfill jobs polled every **5 seconds** from `queue_backfill_data` table

## Key Features

### Multi-Destination Support

Pipelines support multiple destinations through `pipeline_destinations` join table:

- One pipeline (PostgreSQL source) → Many destinations (Snowflake/PostgreSQL)
- Each destination has independent health tracking via `pipeline_metadata`
- Destination-specific errors stored in `pipeline_destinations.error_message`
- Web UI shows destinations array: `pipeline.destinations[].destination.name`

### Backfill Feature

Historical data sync using DuckDB for efficient batch processing:

- Create jobs via `POST /pipelines/{id}/backfill` with optional WHERE filters (max 5)
- Job lifecycle: PENDING → EXECUTING → COMPLETED/FAILED/CANCELLED
- BackfillManager in compute polls `queue_backfill_data` every 5s
- Processes 10,000 rows per batch (configurable) to prevent memory issues
- Runs in separate threads with graceful cancellation support

### Dead Letter Queue (DLQ)

Redis Streams-based failure handling for CDC records:

- Failed records stored in Redis with routing metadata (source_id, destination_id, table_name)
- Separate streams per table/destination: `dlq:{source_id}:{table}:{dest_id}`
- Consumer groups ensure at-least-once delivery during recovery
- Managed by `compute/core/dlq_manager.py` with configurable retry strategies
- Recovery endpoints: `POST /pipelines/{id}/destinations/{dest_id}/recover-dlq`

## Development Workflows

### Backend Development

```bash
# Setup (uses uv package manager)
cd backend
uv sync                          # Install dependencies
uv run alembic upgrade head      # Run migrations

# Run
uv run uvicorn app.main:app --reload --host 0.0.0.0 --port 8000

# Testing
uv run pytest tests/             # Run tests
uv run pytest tests/ --cov=app   # With coverage
```

**Database migrations:**

- Create: `uv run alembic revision --autogenerate -m "description"`
- Apply: `uv run alembic upgrade head`
- Schema lives in `app/domain/models/`

### Web Development

```bash
cd web
pnpm install        # Install dependencies
pnpm dev           # Dev server (Vite)
pnpm build         # Production build
pnpm lint          # ESLint
pnpm format        # Prettier
```

**Key files:**

- Routes auto-generated in `src/routeTree.gen.ts` by TanStack Router plugin
- API client: `src/repo/client.ts` (axios instance with base URL)
- UI components: `src/components/ui/` (shadcn/ui with RTL modifications)

### Compute Development

```bash
cd compute
python -m venv venv && source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
python main.py                  # Requires CONFIG_DATABASE_URL env var
```

## Critical Integration Points

### Configuration Database

All three services read from shared PostgreSQL config tables (`sources`, `destinations`, `pipelines`, `pipeline_metadata`). Schema defined in `migrations/001_create_table.sql`.

**Key workflow:**

1. Backend API writes pipeline configs to DB
2. Compute service polls DB every 10s for `status='START'` pipelines
3. Compute updates `pipeline_metadata.health_status` and `wal_size` during execution

### Pipeline Status Flow

```
Backend API → Set pipeline.status='START'
           → Creates/updates pipeline_destinations records for each destination
           → Each destination gets separate pipeline_metadata entry
Compute    → Detects status='START' 
           → Spawns isolated Process (multiprocessing.Process)
           → Single Debezium connector replicates to ALL destinations
           → Updates pipeline_metadata.health_status per destination
           → Failed records sent to DLQ (Redis Streams)
```

### Snowflake Authentication

Uses **RSA key-pair authentication** (not passwords):

- Private key stored in `destinations.config.snowflake_private_key_path`
- Keys encrypted with passphrase in `snowflake_private_key_passphrase`
- Backend decrypts using `app.core.security.decrypt_value()`

## Common Pitfalls

1. **Backend**: Don't bypass services - always use dependency injection from `app.api.deps`
2. **Backend**: Pipeline creation forces `status='PAUSE'` initially - must explicitly call `/start` endpoint
3. **Web**: Forgot 300ms delay before `invalidateQueries` causes stale UI
4. **Web**: Direct axios imports break - use `api` from `src/repo/client.ts`
5. **Compute**: Changes to config require Compute service restart (polls DB, doesn't listen)
6. **All**: Port conflicts - Backend:8000, Compute:8001, Web:5173, Config DB:5433, Source DB:5434

## Docker Setup

```bash
docker-compose up -d    # Starts config PostgreSQL (port 5433) and source PostGIS (port 5434)
```

This starts:
- **Config DB** on port 5433 (for pipeline configurations)
- **Source DB** on port 5434 (PostGIS-enabled, for CDC source)
- **Redis** on port 6379 (for DLQ)

Both DBs configured with `wal_level=logical` for CDC support.

## Testing Patterns

### Backend Tests

Located in `backend/tests/`. Use pytest with async fixtures:

```python
@pytest.mark.asyncio
async def test_create_pipeline(db_session):
    service = PipelineService(db_session)
    # Test implementation
```

### Web Tests

Primarily manual testing via UI. Table components follow consistent patterns in `features/*/components/`.

## Key Dependencies

- **Backend**: FastAPI, SQLAlchemy 2.0, Pydantic v1, asyncpg, Alembic
- **Web**: React 19, TanStack Router/Query/Table, shadcn/ui, Zod, axios
- **Compute**: pydbzengine (Debezium), psycopg2-binary, httpx

## Environment Variables

Each service has separate config:

- **Backend**: `DATABASE_URL`, `SECRET_KEY`, `WAL_MONITOR_INTERVAL_SECONDS`
- **Compute**: `CONFIG_DATABASE_URL`, `DEBUG`, `LOG_LEVEL`, `REDIS_URL`, `DLQ_KEY_PREFIX`, `DLQ_CHECK_INTERVAL`, `PIPELINE_POOL_MAX_CONN`
- **Web**: `VITE_API_URL` (defaults to http://localhost:8000/api/v1)
