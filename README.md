# Rosetta ETL Platform

Rosetta is a real-time ETL platform with a modular three-service architecture for managing and executing Change Data Capture (CDC) pipelines from **PostgreSQL** to **Snowflake**, **PostgreSQL**.

## Architecture Overview

The platform consists of three independent services:

- **Backend** (FastAPI/Python): RESTful API for managing sources, destinations, and pipeline configurations
- **Compute** (Python/Debezium): CDC execution engine that replicates data changes in real-time
- **Web** (React/TypeScript): Admin dashboard for pipeline monitoring and management

## Project Flow

Data flows through the system in real-time:

1.  **Configuration (Backend API)**: Define sources, destinations, and pipelines via REST API or Web UI
2.  **Source (PostgreSQL)**: Compute service connects to PostgreSQL using logical replication slots to capture WAL changes (INSERT, UPDATE, DELETE)
3.  **Processing (Compute/Debezium)**: The pydbzengine processes CDC events and manages data transformation
4.  **Authentication**: Uses RSA Key-Pair Authentication to securely connect to Snowflake with encrypted private keys (PKCS#8)
5.  **Destination (Snowflake)**: Processed data is ingested into specified Snowflake tables

## System Architecture

Rosetta uses a shared configuration database pattern where all three services read/write to a central PostgreSQL database.

The system schema is defined in `migrations/001_create_table.sql`:

*   **sources**: PostgreSQL source connection configurations
*   **destinations**: Snowflake destination connection details
*   **pipelines**: Pipeline definitions linking sources to destinations
*   **pipeline_metadata**: Real-time status, health metrics, and WAL monitoring data

**Service Communication:**
- Backend API writes pipeline configurations to the database
- Compute service polls the database every 10s for `status='START'` pipelines
- Web dashboard fetches data via Backend API endpoints

## How to Run

### Prerequisites

*   **Python 3.11+** with [uv](https://docs.astral.sh/uv/) package manager (for Backend and Compute)
*   **Node.js 18+** with [pnpm](https://pnpm.io/) (for Web)
*   **Docker** & Docker Compose (for running local PostgreSQL instances)
*   **OpenSSL** (for generating Snowflake authentication keys)

### Step 1: Generate Private & Public Keys

Rosetta uses Key-Pair Authentication for Snowflake.

1.  **Generate Encrypted Private Key** (remember the passphrase):
    ```bash
    openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -v2 des3 -out rsa_key.p8
    ```
2.  **Generate Public Key**:
    ```bash
    openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
    ```
3.  **Configure Snowflake User**:
    ```sql
    ALTER USER <YOUR_USER> SET RSA_PUBLIC_KEY='<CONTENT_OF_RSA_KEY_PUB>';
    ```

### Step 2: Set Environment Variables
environment files for each service:

**Backend** (`backend/.env`):
```bash
DATABASE_URL=postgresql+asyncpg://postgres:postgres@localhost:5433/postgres
SECRET_KEY=your-secret-key-here
WAL_MONITOR_INTERVAL_SECONDS=300
```

**Compute** (`compute/.env`):
```bash
CONFIG_DATABASE_URL=postgres://postgres:postgres@localhost:5433/postgres
DEBUG=falseStart Docker Services

Start the configuration and source PostgreSQL databases:

```bash
docker-compose up -d
```

This starts:
- **Config DB** on port 5433 (for pipeline configurations)
- **Source DB** on port 5434 (PostGIS-enabled, for CDC source)

Both databases are configured with `wal_level=logical` for CDC support.

### Step 4: Setup and Run Backend

```bash
cd backend
uv sync                          # Install dependencies
uv run alembic upgrade head      # Apply database migrations
uv run uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

Backend API will be available at `http://localhost:8000/docs`

### Step 5: Setup and Run Compute

```bash
cd compute
python -m venv venv
source venv/bin/activate         # Windows: venv\Scripts\activate
pip install -r requirements.txt
python main.py
```

Compute service will start polling for active pipelines.

### Step 6: Setup and Run Web

```bash
cd web
pnpm install
pnpm dev
```

Web dashboard will be available at `http://localhost:5173`

### Via Web UI
Use the dashboard at `http://localhost:5173` to manage pipelines through a visual interface.

### Via API
Backend provides REST endpoints at `http://localhost:8000/api/v1`:
- `POST /pipelines` - Create pipeline
- `POST /pipelines/{id}/start` - Start pipeline
- `POST /pipelines/{id}/pause` - Pause pipeline
- `GET /pipelines` - List all pipelines

### Via Direct SQL
Compute service polls for status changes, so you can also control pipelines via SQL:

*   **Pause**: `UPDATE pipelines SET status = 'PAUSE' WHERE name = '...';`
*   **Start**: `UPDATE pipelines SET status = 'START' WHERE name = '...';`

Monitor pipeline health:
```sql
SELECT p.name, pm.health_status, pm.wal_size, pm.last_success_time
FROM pipelines p
JOIN pipeline_metadata pm ON p.id = pm.pipeline_id;
```

## Testing

### Backend Tests
```bash
cd backend
uv run pytest tests/             # Run all tests
uv run pytest tests/ --cov=app   # With coverage report
```

### Compute Tests
```bash
cd compute
pytest tests/
```

## Port Reference

| Service    | Port | Description                    |
|------------|------|--------------------------------|
| Backend    | 8000 | FastAPI REST API               |
| Compute    | 8001 | Health check endpoint          |
| Web        | 5173 | Vite dev server                |
| Config DB  | 5433 | PostgreSQL config database     |
| Source DB  | 5434 | PostgreSQL source (PostGIS)    |

## Documentation

- **Backend**: See `backend/ARCHITECTURE.md` for Clean Architecture details
- **Backend**: See `backend/GETTING_STARTED.md` for API usage
- **Web**: Based on [shadcn-admin](https://github.com/satnaing/shadcn-admin) template
- **AI Agents**: See `.github/copilot-instructions.md` for development guidelinesple SQL configuration can be found in `migrations/002_seed_data.sql
### Step 4: Run Rosetta

Start the local database (if using Docker) and run the application:

```bash
docker-compose up -d
cargo run
```

The application will connect to the `CONFIG_DATABASE_URL`, apply necessary migrations automatically, and start any pipelines marked as `START`.

## Pipeline Management

Rosetta listens for changes in the `pipelines` table. You can control streams in real-time using SQL:

*   **Stop a Pipeline**: `UPDATE pipelines SET status = 'PAUSE' WHERE name = '...';`
*   **Resume/Start**: `UPDATE pipelines SET status = 'START' WHERE name = '...';`
*   **Force Restart**: `UPDATE pipelines SET status = 'REFRESH' WHERE name = '...';`

Check `pipeline_metadata` for errors and status:
```sql
SELECT * FROM pipeline_metadata;
```
