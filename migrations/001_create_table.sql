-- ETL Stream Configuration Database Schema

-- Table 1: Sources (PostgreSQL connection configurations)
CREATE TABLE IF NOT EXISTS sources (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    pg_host VARCHAR(255) NOT NULL,
    pg_port INTEGER NOT NULL DEFAULT 5432,
    pg_database VARCHAR(255) NOT NULL,
    pg_username VARCHAR(255) NOT NULL,
    pg_password VARCHAR(255),
    publication_name VARCHAR(255) NOT NULL,
    replication_id INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Table 2: Destinations Snowflake
CREATE TABLE IF NOT EXISTS destinations (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    snowflake_account VARCHAR(255),
    snowflake_user VARCHAR(255),
    snowflake_database VARCHAR(255),
    snowflake_schema VARCHAR(255),
    snowflake_role VARCHAR(255),
    snowflake_private_key_path VARCHAR(255),
    snowflake_private_key_passphrase VARCHAR(255),
    snowflake_host VARCHAR(255),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Table 3: Pipelines (connects source to destination)
CREATE TABLE IF NOT EXISTS pipelines (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    source_id INTEGER NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    destination_id INTEGER NOT NULL REFERENCES destinations(id) ON DELETE CASCADE,
    status VARCHAR(20) NOT NULL DEFAULT 'START', -- 'START' or 'PAUSE' or 'REFRESH
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Table 4: Pipeline Metadata (contains runtime information)
CREATE TABLE IF NOT EXISTS pipeline_metadata (
    id SERIAL PRIMARY KEY,
    pipeline_id INTEGER NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    status VARCHAR(20) NOT NULL DEFAULT 'RUNNING', -- 'RUNNING' or 'PAUSED'
    last_error TEXT NULL,
    last_error_at TIMESTAMP NULL,
    last_start_at TIMESTAMP NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS system_metrics (
    id SERIAL PRIMARY KEY,
    cpu_usage FLOAT4,        -- Percentage
    total_memory BIGINT,     -- In KB
    used_memory BIGINT,      -- In KB
    total_swap BIGINT,       -- In KB
    used_swap BIGINT,        -- In KB
    recorded_at TIMESTAMPTZ DEFAULT NOW()
);

-- Table 5: WAL Monitor (tracks Write-Ahead Log status per source)
CREATE TABLE IF NOT EXISTS wal_monitor (
    id SERIAL PRIMARY KEY,
    source_id INTEGER NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    wal_lsn VARCHAR(255),           -- Log Sequence Number (e.g., '0/1234ABCD')
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

-- Create indexes for common queries
CREATE INDEX IF NOT EXISTS idx_pipelines_status ON pipelines(status);
CREATE INDEX IF NOT EXISTS idx_pipelines_source_id ON pipelines(source_id);
CREATE INDEX IF NOT EXISTS idx_pipelines_destination_id ON pipelines(destination_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_metadata_pipeline_id ON pipeline_metadata(pipeline_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_metadata_status ON pipeline_metadata(status);
CREATE INDEX IF NOT EXISTS idx_wal_monitor_source_id ON wal_monitor(source_id);
CREATE INDEX IF NOT EXISTS idx_wal_monitor_status ON wal_monitor(status);
CREATE INDEX IF NOT EXISTS idx_wal_monitor_last_received ON wal_monitor(last_wal_received);
