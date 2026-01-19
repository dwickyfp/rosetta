-- Snowflake DDL for tbl_sales landing table
-- Database: DEVELOPMENT
-- Schema: ETL_SCHEMA

-- Create Database if not exists
CREATE DATABASE IF NOT EXISTS DEVELOPMENT;

-- Create Schema if not exists
CREATE SCHEMA IF NOT EXISTS DEVELOPMENT.ETL_SCHEMA;

-- Use the schema
USE SCHEMA DEVELOPMENT.ETL_SCHEMA;

-- Create Landing Table with Schema Evolution enabled
CREATE TABLE IF NOT EXISTS LANDING_TBL_SALES (
    -- Identity & Keys
    sale_id             NUMBER(38,0),  -- BIGINT equivalent in Snowflake
    transaction_uuid    VARCHAR(36),   -- UUID as string
    
    -- Text & Categorical
    customer_name       VARCHAR(100),
    sales_channel       VARCHAR(50),
    region_code         VARCHAR(3),
    
    -- Temporal (Time columns)
    transaction_date    DATE,
    created_at          TIMESTAMP_TZ,
    
    -- Boolean & Status
    is_vip_customer     BOOLEAN,
    is_refunded         BOOLEAN,
    
    -- Numeric variations (Scale & Precision)
    quantity            NUMBER(38,0),       -- INTEGER equivalent
    unit_price          NUMBER(12,2),       -- Same precision as PostgreSQL
    discount_pct        NUMBER(5,4),        -- Same precision
    tax_amount          FLOAT,              -- DOUBLE PRECISION equivalent
    shipping_weight_kg  FLOAT,              -- REAL equivalent
    exchange_rate       NUMBER(18,8),       -- Same precision
    
    -- Advanced Types
    tags                ARRAY,              -- Array type in Snowflake
    metadata            VARIANT,            -- JSON/JSONB equivalent in Snowflake
    
    -- CDC Columns (Change Data Capture)
    operation           VARCHAR(1),         -- C (Create/Insert), U (Update), D (Delete)
    sync_timestamp_rosetta      TIMESTAMP_TZ        -- When data was synced to Snowflake
)
COMMENT = 'Landing table for tbl_sales with CDC support'
ENABLE_SCHEMA_EVOLUTION = TRUE;

-- Create Stream for CDC tracking (optional but recommended for downstream processing)
CREATE OR REPLACE STREAM LANDING_TBL_SALES_STREAM 
ON TABLE LANDING_TBL_SALES
COMMENT = 'Stream to track changes in landing table';

-- Add time-travel retention (optional - default is 1 day)
ALTER TABLE LANDING_TBL_SALES 
SET DATA_RETENTION_TIME_IN_DAYS = 7;

-- Clustering key for better query performance (optional but recommended)
ALTER TABLE LANDING_TBL_SALES 
CLUSTER BY (transaction_date, operation);

-- Grant permissions (adjust as needed for your environment)
GRANT SELECT, INSERT ON TABLE LANDING_TBL_SALES TO ROLE ACCOUNTADMIN;
GRANT SELECT ON STREAM LANDING_TBL_SALES_STREAM TO ROLE ACCOUNTADMIN;

-- Example query to verify table structure
-- SELECT * FROM LANDING_TBL_SALES LIMIT 10;

-- Example query to check CDC operations
-- SELECT operation, COUNT(*) 
-- FROM LANDING_TBL_SALES 
-- WHERE sync_timestamp >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
-- GROUP BY operation;
