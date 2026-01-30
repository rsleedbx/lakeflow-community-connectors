# Changefeed Path Fix - Summary

## What Happened

1. **Initial Observation**: 9 keys missing from target table despite being in Azure
2. **First Diagnosis**: Found CDC files at `.../{source_table}/` instead of `.../{source_table}/{target_table}/`
3. **Wrong Conclusion**: Assumed notebook path construction was incorrect
4. **Wrong Fix**: Removed `/{target_table}` suffix from `cockroachdb_autoload.py`
5. **User Correction**: Notebook path IS correct - changefeed was created wrong!
6. **Correct Fix**: Reverted changes and identified root cause

---

## Root Cause

The CockroachDB changefeed was created with an **incomplete path** missing the `/{target_table}` suffix.

**Correct Path** (notebook design):
```
parquet/{source_catalog}/{source_schema}/{source_table}/{target_table}/
```

**Actual Changefeed Path** (what was created):
```
parquet/{source_catalog}/{source_schema}/{source_table}/
```

---

## Why `/{target_table}` Exists

Enables **multiple Databricks targets** to ingest from the same CockroachDB source without conflicts:

```
parquet/defaultdb/public/orders/
  ├── prod_orders/         ← Production target (isolated)
  ├── staging_orders/      ← Staging target (isolated)
  └── analytics_orders/    ← Analytics target (isolated)
```

Without the suffix, all targets would process the same files → race conditions and conflicts.

---

## Changes Made

### 1. Reverted `cockroachdb_autoload.py`

All 4 ingestion functions now use the CORRECT path with `/{target_table}`:

**Lines updated**: 58, 164, 820, 969

```python
source_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/parquet/{source_catalog}/{source_schema}/{source_table}/{target_table}"
```

### 2. Fixed `cockroachdb_debug.py`

Updated diagnosis tool to use correct path (line 1190):

```python
azure_cdc_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/parquet/{source_catalog}/{source_schema}/{source_table}/{target_table}"
```

### 3. Enhanced Diagnosis Tool

Added automatic Azure file investigation for `append_only` mode when source keys are missing from target.

---

## Next Steps for User

### 1. Drop Existing Changefeed

```sql
-- Find job ID
SHOW CHANGEFEED JOBS;

-- Cancel the changefeed with wrong path
CANCEL JOB <job_id>;
```

### 2. Clean Up Incorrect CDC Files

```python
# Delete files at wrong location
incorrect_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/parquet/{source_catalog}/{source_schema}/{source_table}/"

try:
    dbutils.fs.rm(incorrect_path, recurse=True)
    print(f"✅ Deleted incorrect CDC files at: {incorrect_path}")
except Exception as e:
    print(f"⚠️  Could not delete (may not exist): {e}")
```

### 3. Drop Target Table and Checkpoint

```python
# Drop target table
spark.sql(f"DROP TABLE IF EXISTS {target_table_fqn}")

# Clear checkpoint
dbutils.fs.rm(f"/checkpoints/{target_schema}_{target_table}", recurse=True)

print("✅ Target table and checkpoint cleared")
```

### 4. Recreate Changefeed (Cell 8)

Re-run Cell 8 - it will create a changefeed with the CORRECT path:
```python
path = f"parquet/{source_catalog}/{source_schema}/{source_table}/{target_table}"
```

### 5. Verify Changefeed Path

```sql
-- Check the new changefeed
SELECT job_id, description 
FROM [SHOW CHANGEFEED JOBS] 
WHERE status = 'running';

-- Verify path includes /{target_table}
```

### 6. Wait for CDC Files

```python
# Cell 11 - Wait for files to appear
check_azure_files(...)
```

Should now show files at:
```
parquet/defaultdb/public/usertable_append_only_multi_cf/usertable_append_only_multi_cf/
```

### 7. Re-run Ingestion (Cell 14)

Auto Loader will now find the files in the correct location.

### 8. Verify All Keys Present

```python
df = spark.read.table(target_table_fqn)
print(f"✅ Row count: {df.count()}")  # Should be 20

# Check previously missing keys
missing_keys = [41, 42, 43, 44, 45, 46, 47, 48, 49]
found = df.filter(df.ycsb_key.isin(missing_keys)).count()
print(f"✅ Previously missing keys found: {found}/9")
```

---

## Files Updated

✅ `cockroachdb_autoload.py` - Reverted to correct path
✅ `cockroachdb_debug.py` - Fixed to use correct path
✅ `BUG_FIX_CHANGEFEED_PATH_MISMATCH.md` - Documented correct understanding
✅ `CHANGEFEED_PATH_FIX_SUMMARY.md` - This file

❌ `BUG_FIX_WRONG_SOURCE_PATH.md` - OBSOLETE (wrong diagnosis)
❌ `PATH_REFACTORING_PLAN.md` - OBSOLETE (based on wrong diagnosis)

---

## Key Takeaway

**Don't assume code is wrong just because data is in an unexpected location!**

Always verify:
1. What is the design intent?
2. How was the data created?
3. Could the data be wrong instead of the code?

In this case:
- ✅ Code was correct
- ❌ Changefeed was created incorrectly

---

## Date

2026-01-30
