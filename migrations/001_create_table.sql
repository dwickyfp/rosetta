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
    replication_name VARCHAR(255) NOT NULL,
    is_publication_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    is_replication_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    last_check_replication_publication TIMESTAMPTZ NULL,
    total_tables INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
-- Drop constraint unique
ALTER TABLE sources DROP CONSTRAINT IF EXISTS unique_replication_name;
ALTER TABLE sources DROP CONSTRAINT IF EXISTS unique_publication_name;



-- Table 2: Destinations Snowflake
CREATE TABLE IF NOT EXISTS destinations (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    type VARCHAR(50) NOT NULL DEFAULT 'SNOWFLAKE',
    config JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Table 3: Pipelines (connects source to destination)
CREATE TABLE IF NOT EXISTS pipelines (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,-- 'SNOWFLAKE' or 'POSTGRESQL'
    source_id INTEGER NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    status VARCHAR(20) NOT NULL DEFAULT 'PAUSE', -- 'START' or 'PAUSE' or 'REFRESH
    ready_refresh BOOLEAN NOT NULL DEFAULT FALSE,
    last_refresh_at TIMESTAMPTZ NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Alter table pipelines add column ready_refresh if not exists
ALTER TABLE pipelines ADD COLUMN IF NOT EXISTS ready_refresh BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE pipelines ADD COLUMN IF NOT EXISTS last_refresh_at TIMESTAMPTZ NULL;

-- 1 pipelines sources, now can have more then 1 destination
CREATE TABLE IF NOT EXISTS pipelines_destination (
    id SERIAL PRIMARY KEY,
    pipeline_id INTEGER NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    destination_id INTEGER NOT NULL REFERENCES destinations(id) ON DELETE CASCADE,
    is_error BOOLEAN NOT NULL DEFAULT FALSE,
    error_message TEXT NULL,
    last_error_at TIMESTAMPTZ NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Table Metadata Sync Postgres to Postgres
CREATE TABLE IF NOT EXISTS pipelines_destination_table_sync(
    id SERIAL PRIMARY KEY,
    pipeline_destination_id INTEGER NOT NULL REFERENCES pipelines_destination(id) ON DELETE CASCADE,
    table_name VARCHAR(255) NOT NULL,
    table_name_target VARCHAR(255) NOT NULL,
    custom_sql TEXT NULL,
    filter_sql TEXT NULL,
    is_exists_table_landing BOOLEAN DEFAULT FALSE, -- table landing in snowflake
    is_exists_stream BOOLEAN DEFAULT FALSE, -- stream in snowflake
    is_exists_task BOOLEAN DEFAULT FALSE, -- task in snowflake
    is_exists_table_destination BOOLEAN DEFAULT FALSE, -- table destination in snowflake
    is_error BOOLEAN NOT NULL DEFAULT FALSE,
    error_message TEXT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Table 4: Pipeline Metadata (contains runtime information)
CREATE TABLE IF NOT EXISTS pipeline_metadata (
    id SERIAL PRIMARY KEY,
    pipeline_id INTEGER NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    status VARCHAR(20) NOT NULL DEFAULT 'RUNNING', -- 'RUNNING' or 'PAUSED'
    last_error TEXT NULL,
    last_error_at TIMESTAMPTZ NULL,
    last_start_at TIMESTAMPTZ NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
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
    last_wal_received TIMESTAMPTZ,   -- Last time WAL data was received
    last_transaction_time TIMESTAMPTZ, -- Last transaction timestamp
    replication_slot_name VARCHAR(255), -- Name of the replication slot
    replication_lag_bytes BIGINT,   -- Replication lag in bytes
    total_wal_size VARCHAR(255),    -- Total size of WAL files (e.g., '640 MB')
    status VARCHAR(20) DEFAULT 'ACTIVE', -- 'ACTIVE', 'IDLE', 'ERROR'
    error_message TEXT,             -- Error details if any
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT unique_source_wal UNIQUE (source_id) -- Ensures 1 source = 1 row
);

-- Table 5A: WAL Metrics (stores historical WAL size data)
CREATE TABLE IF NOT EXISTS wal_metrics (
    id SERIAL PRIMARY KEY,
    source_id INTEGER NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    size_bytes BIGINT NOT NULL,
    recorded_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_wal_metrics_source_id ON wal_metrics(source_id);
CREATE INDEX IF NOT EXISTS idx_wal_metrics_recorded_at ON wal_metrics(recorded_at);

-- Save lisat table based on publication, schema table, check table name
CREATE TABLE IF NOT EXISTS table_metadata_list (
    id SERIAL PRIMARY KEY,
    source_id INTEGER NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    table_name VARCHAR(255),
    schema_table JSONB NULL,
    is_changes_schema BOOLEAN DEFAULT FALSE, -- track changes schema
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- track schema changes based on table in table_metadata_list (Append Only)
CREATE TABLE IF NOT EXISTS history_schema_evolution (
    id SERIAL PRIMARY KEY,
    table_metadata_list_id INTEGER NOT NULL REFERENCES table_metadata_list(id) ON DELETE CASCADE,
    schema_table_old JSONB NULL,
    schema_table_new JSONB NULL,
    changes_type VARCHAR(20) NULL, -- 'NEW COLUMN', 'DROP COLUMN', 'CHANGES TYPE', 
    version_schema INTEGER NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS presets (
    id SERIAL PRIMARY KEY,
    source_id INTEGER NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    table_names TEXT[] NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);


-- Create indexes for common queries
CREATE INDEX IF NOT EXISTS idx_pipelines_status ON pipelines(status);
CREATE INDEX IF NOT EXISTS idx_pipelines_source_id ON pipelines(source_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_metadata_pipeline_id ON pipeline_metadata(pipeline_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_metadata_status ON pipeline_metadata(status);
CREATE INDEX IF NOT EXISTS idx_wal_monitor_source_id ON wal_monitor(source_id);
CREATE INDEX IF NOT EXISTS idx_wal_monitor_status ON wal_monitor(status);
CREATE INDEX IF NOT EXISTS idx_wal_monitor_last_received ON wal_monitor(last_wal_received);
CREATE INDEX IF NOT EXISTS idx_table_metadata_list_source_id ON table_metadata_list(source_id);
CREATE INDEX IF NOT EXISTS idx_table_metadata_list_table_name ON table_metadata_list(table_name);
CREATE INDEX IF NOT EXISTS idx_history_schema_evolution_table_metadata_list_id ON history_schema_evolution(table_metadata_list_id);
CREATE INDEX IF NOT EXISTS idx_history_schema_evolution_version_schema ON history_schema_evolution(version_schema);

-- Table 6: Pipeline Progress (tracks initialization progress)
CREATE TABLE IF NOT EXISTS pipelines_progress (
    id SERIAL PRIMARY KEY,
    pipeline_id INTEGER NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    progress INTEGER NOT NULL DEFAULT 0, -- 0 to 100
    step VARCHAR(255), -- current step description e.g. "Creating Landing Table"
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING', -- 'PENDING', 'IN_PROGRESS', 'COMPLETED', 'FAILED'
    details TEXT, -- JSON or text details about the progress
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pipelines_progress_pipeline_id ON pipelines_progress(pipeline_id);

CREATE TABLE IF NOT EXISTS credit_snowflake_monitoring(
    id SERIAL PRIMARY KEY,
    destination_id INTEGER NOT NULL REFERENCES destinations(id) ON DELETE CASCADE,
    total_credit NUMERIC(38, 9) NOT NULL,
    usage_date TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_credit_snowflake_monitoring_destination_id ON credit_snowflake_monitoring(destination_id);

-- Goals is to track record count of each table in each pipeline
CREATE TABLE IF NOT EXISTS data_flow_record_monitoring(
    id SERIAL PRIMARY KEY,
    pipeline_id INTEGER NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    pipeline_destination_id INTEGER NULL REFERENCES pipelines_destination(id) ON DELETE CASCADE,
    source_id  INTEGER NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    pipeline_destination_table_sync_id INTEGER NOT NULL REFERENCES pipelines_destination_table_sync(id) ON DELETE CASCADE,
    table_name VARCHAR(255) NOT NULL,
    record_count BIGINT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Ensure columns exist (for schema evolution on existing tables)
ALTER TABLE data_flow_record_monitoring ADD COLUMN IF NOT EXISTS pipeline_destination_id INTEGER NULL REFERENCES pipelines_destination(id) ON DELETE CASCADE;
ALTER TABLE data_flow_record_monitoring ADD COLUMN IF NOT EXISTS pipeline_destination_table_sync_id INTEGER NULL REFERENCES pipelines_destination_table_sync(id) ON DELETE CASCADE;

CREATE INDEX IF NOT EXISTS idx_data_flow_record_monitoring_pipeline_id ON data_flow_record_monitoring(pipeline_id);
CREATE INDEX IF NOT EXISTS idx_data_flow_record_monitoring_pipeline_destination_id ON data_flow_record_monitoring(pipeline_destination_id);


CREATE TABLE IF NOT EXISTS rosetta_setting_configuration(
    id SERIAL PRIMARY KEY,
    config_key VARCHAR(255) NOT NULL UNIQUE,
    config_value VARCHAR(255) NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO rosetta_setting_configuration(config_key, config_value) VALUES('WAL_MONITORING_THRESHOLD_WARNING', '3000') ON CONFLICT(config_key) DO NOTHING;
INSERT INTO rosetta_setting_configuration(config_key, config_value) VALUES('WAL_MONITORING_THRESHOLD_ERROR', '6000') ON CONFLICT(config_key) DO NOTHING;
INSERT INTO rosetta_setting_configuration(config_key, config_value) VALUES('ENABLE_ALERT_NOTIFICATION_WEBHOOK', 'FALSE') ON CONFLICT(config_key) DO NOTHING;
INSERT INTO rosetta_setting_configuration(config_key, config_value) VALUES('ALERT_NOTIFICATION_WEBHOOK_URL', '') ON CONFLICT(config_key) DO NOTHING;
INSERT INTO rosetta_setting_configuration(config_key, config_value) VALUES('NOTIFICATION_ITERATION_DEFAULT', '3') ON CONFLICT(config_key) DO NOTHING;

-- SETTING FOR BATCH 
INSERT INTO rosetta_setting_configuration(config_key, config_value) VALUES('PIPELINE_MAX_BATCH_SIZE', '4096') ON CONFLICT(config_key) DO NOTHING;
INSERT INTO rosetta_setting_configuration(config_key, config_value) VALUES('PIPELINE_MAX_QUEUE_SIZE', '16384') ON CONFLICT(config_key) DO NOTHING;

-- NEW INDEX
CREATE INDEX IF NOT EXISTS idx_table_metadata_list_source_table ON table_metadata_list(source_id, table_name);
CREATE INDEX IF NOT EXISTS idx_data_flow_record_monitoring_created_at ON data_flow_record_monitoring(created_at);
CREATE INDEX IF NOT EXISTS idx_credit_snowflake_monitoring_usage_date ON credit_snowflake_monitoring(usage_date);

-- Add unique constraint to table_metadata_list (Added retroactively for new deployments)
ALTER TABLE table_metadata_list DROP CONSTRAINT IF EXISTS uq_table_metadata_source_table;
ALTER TABLE table_metadata_list ADD CONSTRAINT uq_table_metadata_source_table UNIQUE (source_id, table_name);

CREATE TABLE IF NOT EXISTS job_metrics_monitoring(
    id SERIAL PRIMARY KEY,
    key_job_scheduler VARCHAR(255) NOT NULL ,
    last_run_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
ALTER TABLE job_metrics_monitoring DROP CONSTRAINT IF EXISTS uq_job_metrics_monitoring_key_job_scheduler;
ALTER TABLE job_metrics_monitoring ADD CONSTRAINT uq_job_metrics_monitoring_key_job_scheduler UNIQUE (key_job_scheduler);

-- Constraint for ON CONFLICT support
ALTER TABLE pipeline_metadata DROP CONSTRAINT IF EXISTS uq_pipeline_metadata_pipeline_id;
ALTER TABLE pipeline_metadata ADD CONSTRAINT uq_pipeline_metadata_pipeline_id UNIQUE (pipeline_id);

-- Table Notification
CREATE TABLE IF NOT EXISTS notification_log(
    id SERIAL PRIMARY KEY,
    key_notification VARCHAR(255) NOT NULL,
    title VARCHAR(255) NOT NULL,
    message TEXT NOT NULL,
    type VARCHAR(255) NOT NULL,
    is_read BOOLEAN DEFAULT FALSE,
    is_deleted BOOLEAN DEFAULT FALSE,
    iteration_check INTEGER DEFAULT 0, -- For check iteration job, if 3 then will sent into webhook if is_read is false
    is_sent BOOLEAN DEFAULT FALSE,
    is_force_sent BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create Index for notification_log
CREATE INDEX IF NOT EXISTS idx_notification_log_iteration_check ON notification_log(iteration_check);

-- Table queue backfill data 
CREATE TABLE IF NOT EXISTS queue_backfill_data(
    id SERIAL PRIMARY KEY,
    pipeline_id INTEGER NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    source_id  INTEGER NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    table_name VARCHAR(255) NOT NULL,
    filter_sql TEXT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING', -- 'PENDING', 'EXECUTING', 'COMPLETED', 'FAILED', 'CANCELLED'
    count_record BIGINT NOT NULL DEFAULT 0,
    total_record BIGINT NOT NULL DEFAULT 0,
    resume_attempts INTEGER NOT NULL DEFAULT 0,
    is_error BOOLEAN NOT NULL DEFAULT FALSE,
    error_message TEXT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Ensure error_message column exists (for existing deployments)
ALTER TABLE queue_backfill_data ADD COLUMN IF NOT EXISTS error_message TEXT NULL;
ALTER TABLE queue_backfill_data ADD COLUMN IF NOT EXISTS total_record BIGINT NOT NULL DEFAULT 0;
ALTER TABLE queue_backfill_data ADD COLUMN IF NOT EXISTS resume_attempts INTEGER NOT NULL DEFAULT 0;
ALTER TABLE queue_backfill_data ADD COLUMN IF NOT EXISTS is_error BOOLEAN NOT NULL DEFAULT FALSE;

-- Drop Index if exists
DROP INDEX IF EXISTS idx_queue_backfill_data_pipeline_id;
DROP INDEX IF EXISTS idx_queue_backfill_data_source_id;
DROP INDEX IF EXISTS idx_queue_backfill_data_created_at;
DROP INDEX IF EXISTS idx_queue_backfill_data_updated_at;

-- Create Index for queue_backfill_data
CREATE INDEX IF NOT EXISTS idx_queue_backfill_data_pipeline_id ON queue_backfill_data(pipeline_id);
CREATE INDEX IF NOT EXISTS idx_queue_backfill_data_source_id ON queue_backfill_data(source_id);
CREATE INDEX IF NOT EXISTS idx_queue_backfill_data_created_at ON queue_backfill_data(created_at);
CREATE INDEX IF NOT EXISTS idx_queue_backfill_data_updated_at ON queue_backfill_data(updated_at);
CREATE INDEX IF NOT EXISTS idx_queue_backfill_data_status ON queue_backfill_data(status);

-- Performance indexes for dashboard queries (5 second refresh optimization)

-- System metrics: filtered by recorded_at for date range queries
CREATE INDEX IF NOT EXISTS idx_system_metrics_recorded_at ON system_metrics(recorded_at DESC);

-- Pipelines destination: critical for error tracking and joins
CREATE INDEX IF NOT EXISTS idx_pipelines_destination_pipeline_id ON pipelines_destination(pipeline_id);
CREATE INDEX IF NOT EXISTS idx_pipelines_destination_is_error ON pipelines_destination(is_error) WHERE is_error = TRUE;
CREATE INDEX IF NOT EXISTS idx_pipelines_destination_last_error_at ON pipelines_destination(last_error_at DESC);
CREATE INDEX IF NOT EXISTS idx_pipelines_destination_destination_id ON pipelines_destination(destination_id);

-- Pipeline destination table sync: for detailed sync monitoring
CREATE INDEX IF NOT EXISTS idx_pipelines_destination_table_sync_pipeline_dest_id ON pipelines_destination_table_sync(pipeline_destination_id);
CREATE INDEX IF NOT EXISTS idx_pipelines_destination_table_sync_is_error ON pipelines_destination_table_sync(is_error) WHERE is_error = TRUE;
CREATE INDEX IF NOT EXISTS idx_pipelines_destination_table_sync_table_name ON pipelines_destination_table_sync(table_name);

-- Pipeline metadata: frequent status checks and activity feed
CREATE INDEX IF NOT EXISTS idx_pipeline_metadata_updated_at ON pipeline_metadata(updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_pipeline_metadata_last_start_at ON pipeline_metadata(last_start_at DESC);

-- Data flow monitoring: composite index for common date + table queries
CREATE INDEX IF NOT EXISTS idx_data_flow_record_monitoring_created_table ON data_flow_record_monitoring(created_at DESC, table_name);
CREATE INDEX IF NOT EXISTS idx_data_flow_record_monitoring_source_id ON data_flow_record_monitoring(source_id);

-- Notification log: dashboard notification queries
CREATE INDEX IF NOT EXISTS idx_notification_log_is_read ON notification_log(is_read) WHERE is_read = FALSE;
CREATE INDEX IF NOT EXISTS idx_notification_log_is_deleted ON notification_log(is_deleted) WHERE is_deleted = FALSE;
CREATE INDEX IF NOT EXISTS idx_notification_log_created_at ON notification_log(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_notification_log_is_sent ON notification_log(is_sent) WHERE is_sent = FALSE;

-- WAL monitor: composite index for status filtering
CREATE INDEX IF NOT EXISTS idx_wal_monitor_updated_at ON wal_monitor(updated_at DESC);

-- Sources: for health monitoring and filtering
CREATE INDEX IF NOT EXISTS idx_sources_is_publication_enabled ON sources(is_publication_enabled);
CREATE INDEX IF NOT EXISTS idx_sources_is_replication_enabled ON sources(is_replication_enabled);

-- Destinations: type-based filtering
CREATE INDEX IF NOT EXISTS idx_destinations_type ON destinations(type);

-- Presets: faster source-based lookups
CREATE INDEX IF NOT EXISTS idx_presets_source_id ON presets(source_id);

-- Job metrics: unique constraint already provides index, add updated_at
CREATE INDEX IF NOT EXISTS idx_job_metrics_monitoring_updated_at ON job_metrics_monitoring(updated_at DESC);









