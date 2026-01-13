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

-- Create indexes for common queries
CREATE INDEX IF NOT EXISTS idx_pipelines_status ON pipelines(status);
CREATE INDEX IF NOT EXISTS idx_pipelines_source_id ON pipelines(source_id);
CREATE INDEX IF NOT EXISTS idx_pipelines_destination_id ON pipelines(destination_id);
