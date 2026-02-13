-- Migration: Add unique constraint to prevent duplicate schema versions
-- This prevents race conditions where multiple schema monitor cycles could create
-- duplicate history records for the same table and version.

-- Step 1: Remove any existing duplicate versions (keep the oldest record)
-- This is necessary before adding the unique constraint
DELETE FROM history_schema_evolution a
USING history_schema_evolution b
WHERE a.id > b.id
  AND a.table_metadata_list_id = b.table_metadata_list_id
  AND a.version_schema = b.version_schema;

-- Step 2: Add unique constraint to prevent future duplicates
ALTER TABLE history_schema_evolution
ADD CONSTRAINT uq_history_schema_table_version 
UNIQUE (table_metadata_list_id, version_schema);

-- Step 3: Add comment for documentation
COMMENT ON CONSTRAINT uq_history_schema_table_version ON history_schema_evolution IS 
'Ensures each table has unique version numbers, preventing duplicate schema history records';
