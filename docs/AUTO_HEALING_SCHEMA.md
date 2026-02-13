# Auto-Healing for Tables Without Schemas

## Overview

When you manually add tables to a PostgreSQL publication via SQL (e.g., `ALTER PUBLICATION ... ADD TABLE ...`), Rosetta now **automatically detects and fixes** tables that don't have their schema information saved.

## How It Works

### Automatic Detection

The system checks for tables without schemas in two ways:

1. **Periodic Schema Monitor** (every 60 seconds)
   - Runs automatically in the background
   - Scans all sources with publications enabled
   - Detects and fixes missing schemas

2. **Manual Source Refresh** (user-triggered)
   - Click "Refresh" button on source details page
   - Immediately checks and fixes missing schemas

### What Gets Fixed

When a table without schema is detected, the system:

1. ✅ Fetches the schema from the source database
2. ✅ Saves it to `table_metadata_list.schema_table`
3. ✅ Creates an INITIAL_LOAD history record (version 1)
4. ✅ Logs the fix: `"Fixed table [name]: Added schema ([N] columns)"`

## Example Scenario

### Before

```sql
-- You add a table to publication manually
ALTER PUBLICATION rosetta_publication ADD TABLE orders;
```

**Problem:** Table shows in monitoring table but `schema_table` column is NULL or empty

### After (Automatic)

Within 60 seconds (or immediately if you click "Refresh Source"):

1. Schema monitor detects `orders` table in publication
2. Sees that `schema_table` is NULL
3. Fetches schema from source database
4. Saves: `{"id": {"column_name": "id", "data_type": "BIGINT", ...}, ...}`
5. Creates INITIAL_LOAD history record
6. ✅ **FIXED:** Table now shows version 1 with all columns visible in UI

## Testing

### Quick Test

1. Add a table to your publication:
   ```sql
   ALTER PUBLICATION rosetta_publication ADD TABLE test_table;
   ```

2. Wait 60 seconds or click "Refresh Source" in UI

3. Check the logs:
   ```bash
   tail -f backend/logs/app.log | grep "test_table"
   ```

   You should see:
   ```
   Added new table tracking with schema: test_table (5 columns)
   ```

4. Verify in the UI:
   - Navigate to Sources → [Your Source] → Tables
   - Find `test_table`
   - Should show version 1 with all columns

### Database Verification

```sql
SELECT 
    t.table_name,
    t.schema_table IS NOT NULL as has_schema,
    h.version_schema,
    h.changes_type
FROM table_metadata_list t
LEFT JOIN history_schema_evolution h ON h.table_metadata_list_id = t.id
WHERE t.table_name = 'test_table';
```

Expected result:
- `has_schema`: `true`
- `version_schema`: `1`
- `changes_type`: `INITIAL_LOAD`

## Code Locations

Auto-healing is implemented in:

1. **Backend Schema Monitor**
   - File: `backend/app/domain/services/schema_monitor.py`
   - Method: `sync_table_list()`
   - Runs every 60 seconds automatically

2. **Backend Source Service**
   - File: `backend/app/domain/services/source.py`
   - Method: `_sync_publication_tables()`
   - Runs when user clicks "Refresh Source"

## Logging

Look for these log messages to confirm auto-healing is working:

```
# New table added with schema
Added new table tracking with schema: [table_name] ([N] columns)

# Existing table fixed
Found table [table_name] without schema, fetching now...
Fixed table [table_name]: Added schema and INITIAL_LOAD history ([N] columns)

# Table skipped (no columns or inaccessible)
Skipping table [table_name]: No schema columns found. Table may be empty or inaccessible.

# Error handling
Failed to fetch schema for existing table [table_name]: [error message]
```

## Troubleshooting

### Issue: Table still shows empty schema after 60 seconds

**Check:**
1. Is the schema monitor running? Check logs for "Starting schema monitoring cycle"
2. Is the source publication enabled? Check `sources.is_publication_enabled = true`
3. Does the table exist in the source database? Check `SELECT * FROM pg_tables WHERE tablename = 'your_table';`
4. Can the system access the table? Check permissions with `SELECT has_table_privilege('your_table', 'SELECT');`

**Solution:**
- Try clicking "Refresh Source" in UI to force immediate healing
- Check backend logs for error messages
- Run manual cleanup script: `uv run python backend/scripts/fix_schema_version_issues.py`

### Issue: Schema fetch fails with permission error

**Reason:** User doesn't have SELECT permission on the table

**Solution:**
```sql
GRANT SELECT ON TABLE your_table TO rosetta_user;
```

### Issue: Table has no columns

**Reason:** Table might be a view or has no accessible columns

**Solution:**
- Verify table structure: `\d your_table` in psql
- Check if it's a materialized view or special table type
- Ensure table has at least one column

## Benefits

✅ **No manual intervention needed** - Tables get schemas automatically
✅ **Works for new tables** - Added via SQL or UI
✅ **Fixes existing issues** - Heals tables with missing schemas
✅ **Handles errors gracefully** - Continues processing other tables if one fails
✅ **Logs everything** - Easy to track what was fixed and when
✅ **Works in two modes** - Automatic (periodic) and manual (on-demand)

## Related Documentation

- Full fix details: [SCHEMA_VERSION_FIX.md](./SCHEMA_VERSION_FIX.md)
- Database migration: [007_add_unique_constraint_schema_version.sql](../migrations/007_add_unique_constraint_schema_version.sql)
- Cleanup script: [fix_schema_version_issues.py](../backend/scripts/fix_schema_version_issues.py)
