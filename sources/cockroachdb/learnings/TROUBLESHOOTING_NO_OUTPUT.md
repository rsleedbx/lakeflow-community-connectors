# Troubleshooting: No Output Rows Despite Successful Processing

## Issue Summary
The CockroachDB connector successfully processes and coalesces 10,000 rows, but the pipeline terminates with "Query termination received" and no rows are written to the Delta table.

## Current Status (Attempt #37)

### ✅ What Was Fixed
Modified `read_table_metadata()` to dynamically set `ingestion_type` based on `initial_scan`:

```python
initial_scan = table_options.get("initial_scan", "yes").lower()
ingestion_type = "snapshot" if initial_scan == "only" else "cdc"
```

- **`initial_scan='only'`** → `ingestion_type='snapshot'` → DLT uses `apply_changes_from_snapshot()` (batch mode)
- **`initial_scan='yes'`** → `ingestion_type='cdc'` → DLT uses `apply_changes()` (streaming mode)

### 📊 Latest Pipeline Run
- **Update ID**: `ee25cb62-0e58-40de-baf9-3bf1824daed2`
- **State**: COMPLETED
- **Creation Time**: 2025-12-18T21:40:02.302Z
- **Pipeline URL**: https://e2-dogfood.staging.cloud.databricks.com/pipelines/7d02d189-2d1b-4331-aee7-acf24bcd9225

### ⚠️ Problem
The log file provided (stderr (10).txt) does NOT contain the new pipeline run with the fix. The latest run in that log is `runId=fbc5c329-3e1b-4ab4-8e8e-1375b7a78eb0` which ended at 21:40:27, but this is an older run.

## What to Check Next

### 1. Download Fresh Logs
Download the logs for update ID `ee25cb62-0e58-40de-baf9-3bf1824daed2`. Look for:

```
================================================================================
✅ read_table_metadata() COMPLETED
================================================================================
📊 Metadata returned:
   primary_keys: ['ycsb_key']
   cursor_field: '_cdc_updated'
   ingestion_type: 'snapshot' ← (initial_scan='only')
   column_family_count: 11
================================================================================
```

This debug output confirms:
- The new code is deployed
- `ingestion_type` is correctly set to `'snapshot'`
- DLT will use `apply_changes_from_snapshot()` instead of `apply_changes()`

### 2. Check Output Table
Query the Delta table to see if data was written:

```sql
SELECT COUNT(*) FROM main.robert_lee_cockroachdb.usertable;
SELECT * FROM main.robert_lee_cockroachdb.usertable LIMIT 10;
```

Expected result: 10,000 rows

### 3. Check Table Metadata
```sql
DESCRIBE EXTENDED main.robert_lee_cockroachdb.usertable;
```

Look for:
- Table properties
- Last update timestamp
- Row count (if available)

### 4. Check Pipeline Event Log
In the Databricks UI, check the pipeline's event log for update `ee25cb62-0e58-40de-baf9-3bf1824daed2`:
- Were there any warnings or errors?
- Was the table created/updated?
- What was the final status?

## Key Diagnostic Questions

1. **Was `read_table_metadata()` called?**
   - Look for the debug output showing `ingestion_type='snapshot'`
   - If not present, the new code wasn't deployed

2. **Did DLT recognize the snapshot mode?**
   - Look for DLT logs mentioning `apply_changes_from_snapshot`
   - If it's still using `apply_changes`, the metadata wasn't read correctly

3. **Were rows passed to DLT?**
   - Connector logs show: "Total events returned: 10000"
   - But did Spark receive them?

4. **Is there a schema mismatch?**
   - Check if DLT is rejecting rows due to schema validation
   - Look for any schema-related errors in the logs

## Possible Root Causes

### A. Metadata Not Read (Most Likely)
If the new debug output is missing, it means `read_table_metadata()` wasn't called with the new code. Possible reasons:
- Cached metadata from a previous run
- Code deployment didn't complete
- Wrong connector version loaded

**Fix**: Force a full refresh:
```bash
databricks pipelines reset 7d02d189-2d1b-4331-aee7-acf24bcd9225
databricks pipelines start-update 7d02d189-2d1b-4331-aee7-acf24bcd9225 --full-refresh true
```

### B. DLT Still Using `apply_changes()` (Streaming Mode)
If `ingestion_type='cdc'` was returned (instead of `'snapshot'`), DLT will use streaming mode and expect a continuous stream. When the snapshot ends, it treats it as an error.

**Fix**: Verify the logic in `read_table_metadata()`:
```python
initial_scan = table_options.get("initial_scan", "yes").lower()
ingestion_type = "snapshot" if initial_scan == "only" else "cdc"
```

### C. Schema Validation Failure
If rows are being rejected due to schema mismatches, they won't appear in the output.

**Fix**: Check the schema returned by `get_table_schema()` and ensure all fields are marked as `nullable=True`.

### D. Pipeline Configuration Issue
The pipeline might not be configured to use the connector's `ingestion_type` metadata.

**Fix**: Verify the pipeline configuration calls `_get_table_metadata()` and passes the metadata to DLT correctly.

## Next Steps

1. **Download fresh logs** for update `ee25cb62-0e58-40de-baf9-3bf1824daed2`
2. **Check for the metadata debug output** showing `ingestion_type='snapshot'`
3. **Query the output table** to see if rows were written
4. If metadata output is missing: **Force a full refresh** to clear caches
5. If rows still not output: **Check DLT event log** for errors

## Reference

- **Pipeline ID**: `7d02d189-2d1b-4331-aee7-acf24bcd9225`
- **Latest Update ID**: `ee25cb62-0e58-40de-baf9-3bf1824daed2`
- **Output Table**: `main.robert_lee_cockroachdb.usertable`
- **Connection**: `robert_lee_battle-walrus-11108`
- **Source Database**: CockroachDB `ycsb.public.usertable` (10,000 rows)


