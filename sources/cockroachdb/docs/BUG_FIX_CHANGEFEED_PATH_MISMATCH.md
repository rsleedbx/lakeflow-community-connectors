# Bug Fix: Changefeed Created with Wrong Path

## Summary

**Real Bug**: The CockroachDB changefeed was created with an **incomplete path** (missing `/{target_table}` suffix), causing it to write CDC files to the wrong location in Azure.

**Impact**: Auto Loader couldn't find the files because it was looking in the correct location (`.../{source_table}/{target_table}/`) while CockroachDB was writing to the wrong location (`.../{source_table}/`).

**Root Cause**: Changefeed was created incorrectly at some point, possibly manually or with an older version of the notebook.

---

## The Bug

### Correct Path (Expected)

The notebook is designed to create changefeeds with this path structure:
```
parquet/{source_catalog}/{source_schema}/{source_table}/{target_table}/
```

Example:
```
parquet/defaultdb/public/usertable_append_only_multi_cf/usertable_append_only_multi_cf/
```

### What Actually Happened

The changefeed was writing files to:
```
parquet/defaultdb/public/usertable_append_only_multi_cf/
```

**Missing**: `/{target_table}/` suffix

### Why This Matters

The `/{target_table}/` suffix allows **multiple Databricks targets** to ingest from the **same CockroachDB source table** without conflicts:

```
parquet/defaultdb/public/orders/
  ├── databricks_prod_orders/        ← Production target
  ├── databricks_staging_orders/     ← Staging target
  └── databricks_analytics_orders/   ← Analytics target
```

---

## Evidence from Diagnosis

### Azure Files Location

Diagnosis showed files at:
```
abfss://changefeed-events@...dfs.core.windows.net/parquet/defaultdb/public/usertable_append_only_multi_cf/
```

NOT at the expected:
```
abfss://changefeed-events@...dfs.core.windows.net/parquet/defaultdb/public/usertable_append_only_multi_cf/usertable_append_only_multi_cf/
```

### Auto Loader Was Looking in the Right Place

`cockroachdb_autoload.py` was correctly constructing:
```python
source_path = f".../{source_table}/{target_table}"
```

But finding 0 files because CockroachDB was writing to:
```
.../{source_table}/
```

---

## What I Mistakenly Did

1. **Saw files in Azure without `/{target_table}` suffix**
2. **Assumed the notebook path construction was wrong**
3. **"Fixed" `cockroachdb_autoload.py` by removing `/{target_table}`**
4. **User corrected me** - the notebook path IS correct!

---

## Actual Fix

### REVERTED Changes to `cockroachdb_autoload.py`

All 4 functions now correctly use:
```python
source_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/parquet/{source_catalog}/{source_schema}/{source_table}/{target_table}"
```

### FIXED `cockroachdb_debug.py`

Updated diagnosis tool to use correct path:
```python
azure_cdc_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/parquet/{source_catalog}/{source_schema}/{source_table}/{target_table}"
```

### Need to RECREATE Changefeed

The existing changefeed is writing to the wrong location. Need to:

1. **Drop existing changefeed**:
   ```sql
   CANCEL JOB <job_id>;
   ```

2. **Delete incorrect CDC files in Azure**:
   ```python
   # Clean up files at wrong location
   dbutils.fs.rm(f"abfss://.../{source_table}/", recurse=True)
   ```

3. **Recreate changefeed with correct path** (Cell 8):
   ```python
   path = f"parquet/{source_catalog}/{source_schema}/{source_table}/{target_table}"
   changefeed_path = f"azure://{container_name}/{path}?..."
   ```

4. **Re-run ingestion** (Cell 14):
   Files will now be in the correct location and Auto Loader will find them.

---

## Why the Changefeed Path Was Wrong

Possible causes:
1. **Manual creation**: Changefeed was created manually without `/{target_table}` suffix
2. **Older notebook version**: Earlier version didn't include the suffix
3. **Copy-paste error**: Path was incorrectly copied from documentation
4. **Testing**: Created for quick testing and never updated

---

## Design Pattern: Why `/{target_table}` Exists

### Problem

Multiple Databricks workspaces/environments need to ingest from the same CockroachDB table:
- Production Databricks
- Staging Databricks
- Analytics Databricks

### Without `/{target_table}` Suffix (BAD)

All targets read from the same path → conflicts, duplicate processing, race conditions:
```
parquet/defaultdb/public/orders/
  ├── 2026-01-30/...  ← All 3 targets process same files
  └── 2026-01-31/...
```

### With `/{target_table}` Suffix (GOOD)

Each target has its own isolated path → no conflicts:
```
parquet/defaultdb/public/orders/
  ├── prod_orders/
  │   └── 2026-01-30/...  ← Only production processes these
  ├── staging_orders/
  │   └── 2026-01-30/...  ← Only staging processes these
  └── analytics_orders/
      └── 2026-01-30/...  ← Only analytics processes these
```

---

## Testing After Fix

1. **Verify changefeed path**:
   ```sql
   SELECT description FROM [SHOW CHANGEFEED JOB <job_id>];
   -- Should show: .../{source_table}/{target_table}/
   ```

2. **Check Azure files**:
   ```python
   check_azure_files(...)
   # Should show files in .../{source_table}/{target_table}/
   ```

3. **Run ingestion**:
   ```python
   ingest_cdc_append_only_multi_family(...)
   ```

4. **Verify all keys present**:
   ```python
   df = spark.read.table(target_table_fqn)
   print(f"Row count: {df.count()}")  # Should be 20 (full source count)
   ```

---

## Lesson Learned

**Don't assume the code is wrong just because the data is in an unexpected location!**

The data being in the wrong location might mean:
- ✅ Code is correct
- ❌ Data was created incorrectly
- ❌ Process was run incorrectly
- ❌ Manual intervention happened

Always **verify the design intent** before "fixing" code that's actually correct!

---

## Related Files

- `cockroachdb_autoload.py` (reverted to correct path)
- `cockroachdb_debug.py` (fixed to use correct path)
- `cockroachdb-cdc-tutorial.ipynb` (Cell 8 has correct path)
- `BUG_FIX_WRONG_SOURCE_PATH.md` (OBSOLETE - wrong diagnosis)
- `PATH_REFACTORING_PLAN.md` (OBSOLETE - based on wrong diagnosis)

---

## Date

2026-01-30
