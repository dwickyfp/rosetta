# Schema Version Issue - Fix Documentation

## Problem Description

When manually adding tables to a PostgreSQL publication via SQL (e.g., `ALTER PUBLICATION ... ADD TABLE ...`), the schema monitoring system could create multiple empty or duplicate version records, resulting in:

- Schema version showing as 9 (or higher) but all versions appearing empty
- Duplicate history records with the same version number
- Tables with NULL or empty schema_table field
- Unable to view historical schema versions

## Root Causes

### 1. **No Unique Constraint**

The `history_schema_evolution` table lacked a unique constraint on `(table_metadata_list_id, version_schema)`, allowing duplicate version records to be created during:

- Concurrent schema monitor cycles
- Multiple manual refresh triggers
- Race conditions between monitor runs

### 2. **Schema Not Fetched Immediately**

When `sync_table_list()` detected a new table in the publication, it created a `TableMetadata` record with `schema_table=NULL` without fetching the actual schema. This caused:

- Empty schema records in the database
- Incorrect version counting
- Failed schema comparisons

### 3. **Incorrect Schema Retrieval Logic**

The `get_table_schema_by_version()` method always used `schema_table_old` for historical versions, but for INITIAL_LOAD records (version 1), the schema is stored in `schema_table_new`, resulting in empty results.

### 4. **Missing Validation**

No validation prevented:

- Creating history records with empty schemas
- Creating duplicate INITIAL_LOAD records
- Processing tables with no accessible columns

## Solutions Implemented

### 1. Database Migration (Required)

Run the migration to add a unique constraint and clean up duplicates:

```bash
cd backend
psql -h localhost -p 5433 -U postgres -d rosetta_config -f ../migrations/007_add_unique_constraint_schema_version.sql
```

This migration:

- Removes existing duplicate version records (keeps oldest)
- Adds unique constraint `uq_history_schema_table_version` on `(table_metadata_list_id, version_schema)`
- Prevents future duplicate versions at the database level

### 2. Code Fixes Applied

#### a. **sync_table_list() - Immediate Schema Fetch & Auto-Healing**

[schema_monitor.py](d:\Research\rosetta\backend\app\domain\services\schema_monitor.py)

Enhanced to handle both new and existing tables:

**For New Tables:**

- Immediately fetches the schema using `fetch_table_schema()`
- Validates schema is not empty before creating record
- Creates `TableMetadata` with populated `schema_table`
- Creates INITIAL_LOAD history record (version 1) immediately
- Skips tables with no accessible columns
- Rolls back on fetch errors

**For Existing Tables (Auto-Healing):**

- Checks all existing tables in publication for missing schemas
- Identifies tables with `schema_table=NULL` or `schema_table={}`
- Automatically fetches and saves schema for these tables
- Creates or updates INITIAL_LOAD history record
- Logs each fix for monitoring
- Continues processing even if individual tables fail

This ensures that tables added manually via SQL will have their schemas populated during the next monitor cycle (runs every 60 seconds by default).

#### b. **\_sync_publication_tables() - Schema Healing on Refresh**

[source.py](d:\Research\rosetta\backend\app\domain\services\source.py)

Enhanced to fix existing tables without schemas:

**For New Tables:**

- Fetches schema immediately when creating table metadata
- Validates schema is not empty
- Creates INITIAL_LOAD history record
- Logs the addition with schema size

**For Existing Tables (Auto-Healing):**

- Checks all tracked tables for missing schemas
- Automatically fetches and saves schema for tables in publication
- Creates or updates INITIAL_LOAD history if missing
- Applies when user triggers manual "Refresh Source" in UI
- Handles errors gracefully without blocking other tables

#### c. **fetch_and_compare_schema() - Duplicate Prevention**

[schema_monitor.py](d:\Research\rosetta\backend\app\domain\services\schema_monitor.py)

Added validation to prevent duplicates:

- Checks if INITIAL_LOAD already exists before creating
- Validates fetched schema is not empty
- Checks if version already exists before creating history record
- Prevents race condition duplicates

#### d. **get_table_schema_by_version() - Correct Schema Retrieval**

[source.py](d:\Research\rosetta\backend\app\domain\services\source.py)

Fixed schema retrieval logic:

- For INITIAL_LOAD records, uses `schema_table_new` (not `old`)
- For change records, uses `schema_table_old`
- Validates schema_data is not empty before returning
- Returns empty columns instead of failing

### 3. Database Cleanup Script (Optional)

If you already have corrupted data, run the cleanup script:

```bash
cd backend
uv run python scripts/fix_schema_version_issues.py
```

This script:

- Identifies tables with empty schemas
- Removes duplicate version records (keeps oldest)
- Re-fetches schemas for broken tables
- Creates missing INITIAL_LOAD history records
- Generates a summary report

## Auto-Healing Mechanism

After applying the code fixes, the system will **automatically detect and fix** tables without schemas:

### When Auto-Healing Triggers

1. **Periodic Schema Monitor** (Every 60 seconds by default)
   - Scans all sources with publications enabled
   - Checks each table in publication
   - Detects missing schemas and fetches them automatically
   
2. **Manual Source Refresh** (User-triggered in UI)
   - Click "Refresh" button on source details page
   - System syncs publication tables
   - Auto-heals any tables with missing schemas

### What Auto-Healing Does

When a table without schema is detected:

```
1. Check: Is table in publication? → Yes, continue
2. Check: Does table have schema? → No (NULL or {})
3. Action: Fetch schema from source database
4. Action: Save schema to table_metadata_list.schema_table
5. Action: Create/Update INITIAL_LOAD history record
6. Log: "Fixed table [name]: Added schema ([N] columns)"
```

### Example Scenario

**Problem:**
```sql
-- You manually add a table to publication
ALTER PUBLICATION rosetta_publication ADD TABLE new_orders;
```

**Before Fix:** Table shows in UI with version 9 but empty schema

**After Fix (Automatic):**
1. Schema monitor detects table in publication
2. Schema monitor sees empty schema
3. Fetches schema: `SELECT column_name, data_type... FROM information_schema.columns...`
4. Saves: `{"id": {...}, "customer": {...}, "total": {...}}`
5. Creates history: INITIAL_LOAD version 1
6. ✅ Table now shows version 1 with all columns visible

**Timeline:** Within 60 seconds (next monitor cycle) or immediately if you click "Refresh Source"

### Manual Healing (Optional)

If you can't wait for auto-healing or want to fix all tables at once:

```bash
cd backend
uv run python scripts/fix_schema_version_issues.py
```

## How to Verify the Fix

### 1. Check Unique Constraint

```sql
SELECT constraint_name, constraint_type
FROM information_schema.table_constraints
WHERE table_name = 'history_schema_evolution'
  AND constraint_type = 'UNIQUE';
```

Should return: `uq_history_schema_table_version`

### 2. Check for Duplicate Versions

```sql
SELECT
    table_metadata_list_id,
    version_schema,
    COUNT(*) as count
FROM history_schema_evolution
GROUP BY table_metadata_list_id, version_schema
HAVING COUNT(*) > 1;
```

Should return: 0 rows (no duplicates)

### 3. Check for Empty Schemas

```sql
SELECT
    id,
    table_name,
    CASE
        WHEN schema_table IS NULL THEN 'NULL'
        WHEN schema_table = '{}'::jsonb THEN 'EMPTY'
        ELSE 'OK'
    END as schema_status
FROM table_metadata_list
WHERE schema_table IS NULL OR schema_table = '{}'::jsonb;
```

Should return: 0 rows (all tables have schemas)

### 4. Test Auto-Healing Mechanism

**Test Case 1: New Table Added via SQL**

1. Add a table to publication manually:
```sql
ALTER PUBLICATION rosetta_publication ADD TABLE test_auto_heal;
```

2. Wait 60 seconds for schema monitor cycle (or click "Refresh Source" in UI)

3. Check backend logs for auto-healing:
```bash
tail -f backend/logs/app.log | grep "test_auto_heal"
```

Should see: 
```
Added new table tracking with schema: test_auto_heal (5 columns)
```

4. Verify in database:
```sql
SELECT 
    t.table_name,
    jsonb_array_length(jsonb_agg(jsonb_object_keys(t.schema_table))) as column_count,
    h.changes_type,
    h.version_schema
FROM table_metadata_list t
JOIN history_schema_evolution h ON h.table_metadata_list_id = t.id
WHERE t.table_name = 'test_auto_heal'
GROUP BY t.table_name, h.changes_type, h.version_schema;
```

Should return: `test_auto_heal`, column_count > 0, `INITIAL_LOAD`, version 1

**Test Case 2: Existing Table Without Schema**

1. Simulate a table with missing schema:
```sql
-- Create a table entry without schema
INSERT INTO table_metadata_list (source_id, table_name, schema_table, is_changes_schema)
VALUES (1, 'test_broken_schema', NULL, false);
```

2. Add table to publication:
```sql
ALTER PUBLICATION rosetta_publication ADD TABLE test_broken_schema;
```

3. Wait 60 seconds or click "Refresh Source" in UI

4. Check logs for healing:
```bash
tail -f backend/logs/app.log | grep "test_broken_schema"
```

Should see:
```
Found table test_broken_schema without schema, fetching now...
Fixed table test_broken_schema: Added schema and INITIAL_LOAD history (4 columns)
```

5. Verify schema was populated:
```sql
SELECT 
    table_name,
    CASE 
        WHEN schema_table IS NOT NULL AND schema_table != '{}'::jsonb THEN 'FIXED'
        ELSE 'STILL BROKEN'
    END as status,
    jsonb_object_keys(schema_table) as columns
FROM table_metadata_list
WHERE table_name = 'test_broken_schema';
```

Should return: `test_broken_schema`, `FIXED`, with all columns listed

### 5. Test Manual Table Addition (Complete Flow)

1. Add a table to publication manually:

```sql
ALTER PUBLICATION rosetta_publication ADD TABLE test_table;
```

2. Wait for schema monitor cycle (or trigger refresh in UI)

3. Check table was added correctly:

```sql
SELECT
    t.id,
    t.table_name,
    jsonb_object_keys(t.schema_table) as columns,
    h.version_schema,
    h.changes_type
FROM table_metadata_list t
LEFT JOIN history_schema_evolution h ON h.table_metadata_list_id = t.id
WHERE t.table_name = 'test_table'
ORDER BY h.version_schema;
```

Should show:

- Table with populated schema
- Single INITIAL_LOAD record (version 1)
- All columns listed

### 6. Check Version Display in UI

Navigate to Sources → [Your Source] → Tables

Each table should show:

- Correct version number
- Ability to view each version's schema
- No empty version displays

## Prevention Going Forward

The fixes ensure:

1. **Database Level**: Unique constraint prevents duplicate versions
2. **Application Level**: Validation prevents empty schemas and duplicate records
3. **Race Condition**: Duplicate checks prevent concurrent creation
4. **Error Handling**: Failed schema fetches don't corrupt data

## Troubleshooting

### Issue: Migration fails with "constraint already exists"

**Solution**: The constraint was already added. Run:

```sql
SELECT constraint_name
FROM information_schema.table_constraints
WHERE table_name = 'history_schema_evolution'
  AND constraint_name = 'uq_history_schema_table_version';
```

If it exists, skip the migration.

### Issue: Cleanup script fails to fetch schema

**Reason**: Table may not exist in source database or lacks permissions.

**Solution**:

1. Check table exists: `SELECT * FROM pg_tables WHERE tablename = 'your_table';`
2. Check permissions: `SELECT has_table_privilege('your_table', 'SELECT');`
3. Remove invalid table metadata manually if table was dropped

### Issue: Version still showing as empty in UI

**Steps**:

1. Clear browser cache
2. Check if table actually has schema: `SELECT schema_table FROM table_metadata_list WHERE table_name = 'your_table';`
3. Check history records: `SELECT * FROM history_schema_evolution WHERE table_metadata_list_id = (SELECT id FROM table_metadata_list WHERE table_name = 'your_table');`
4. Run cleanup script to rebuild schema

## Files Changed

1. **backend/app/domain/services/schema_monitor.py**
   - Enhanced `sync_table_list()` to fetch schemas immediately for new tables
   - Added auto-healing for existing tables without schemas
   - Added validation in `fetch_and_compare_schema()`
   - Added duplicate prevention checks

2. **backend/app/domain/services/source.py**
   - Enhanced `_sync_publication_tables()` with auto-healing
   - Fixed `get_table_schema_by_version()` INITIAL_LOAD handling
   - Added schema data validation
   - Added INITIAL_LOAD history creation for new tables

3. **migrations/007_add_unique_constraint_schema_version.sql**
   - New migration for unique constraint

4. **backend/scripts/fix_schema_version_issues.py**
   - New cleanup script for existing data

## Testing Checklist

- [ ] Run database migration (`007_add_unique_constraint_schema_version.sql`)
- [ ] Run cleanup script (if existing data issues)
- [ ] Restart backend service to apply code changes
- [ ] Add table manually to publication via SQL
- [ ] Wait 60 seconds or click "Refresh Source" in UI
- [ ] Check logs for auto-healing message
- [ ] Verify table appears with correct schema in UI
- [ ] Verify only one INITIAL_LOAD record created
- [ ] Verify version 1 displays correctly in UI with all columns
- [ ] Test existing table without schema gets auto-healed
- [ ] Make schema change and verify version 2 created
- [ ] Check no duplicate versions exist (SQL query)
- [ ] Test concurrent schema monitor cycles (no duplicates)
- [ ] Verify auto-healing works on both monitor cycle and manual refresh

## Support

If issues persist after applying all fixes:

1. Check backend logs: `backend/logs/app.log`
2. Look for schema monitor errors
3. Verify database connection and permissions
4. Run SQL queries above to diagnose data state
5. Share findings with development team
