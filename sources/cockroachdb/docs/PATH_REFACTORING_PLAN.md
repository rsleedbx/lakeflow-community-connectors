# Path Refactoring Plan: Pass Instead of Recalculate

## Problem

The CDC file path is being **recalculated** in 4+ different locations, each potentially with different logic, leading to path mismatches and bugs.

**Current approach (BAD)**:
```python
# Notebook creates changefeed
path = f"parquet/{source_catalog}/{source_schema}/{source_table}/{target_table}"  # ❌ WRONG

# Ingestion function recalculates
source_path = f"abfss://{container}/{source_catalog}/{source_schema}/{source_table}"  # ✅ CORRECT

# check_azure_files recalculates  
prefix = f"parquet/{source_catalog}/{source_schema}/{source_table}/{target_table}/"  # ❌ WRONG

# Diagnosis tool recalculates
azure_cdc_path = f"abfss://{container}/{source_catalog}/{source_schema}/{source_table}"  # ✅ CORRECT
```

**Result**: Path mismatch → Auto Loader can't find files → Data loss!

---

## Root Cause

CockroachDB writes CDC files to:
```
parquet/{source_catalog}/{source_schema}/{source_table}/
```

**NOT** to:
```
parquet/{source_catalog}/{source_schema}/{source_table}/{target_table}/
```

The `/{target_table}` suffix in the changefeed URI is ignored by CockroachDB!

---

## Solution: Pass Path as Parameter

### 1. Define Path Once (in Notebook)

```python
# Cell 3: Configuration
# Define the CDC path ONCE based on actual CockroachDB behavior
cdc_relative_path = f"parquet/{source_catalog}/{source_schema}/{source_table}"
cdc_azure_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{cdc_relative_path}"
```

### 2. Update Changefeed Creation (Cell 8)

**Before**:
```python
path = f"parquet/{source_catalog}/{source_schema}/{source_table}/{target_table}"  # ❌ WRONG
changefeed_path = f"azure://{container_name}/{path}?..."
```

**After**:
```python
# Use the standardized path (without target_table suffix)
changefeed_path = f"azure://{container_name}/{cdc_relative_path}?..."
```

### 3. Update Ingestion Functions

**Add `source_path` parameter** to all 4 functions:

```python
def ingest_cdc_append_only_single_family(
    storage_account_name, container_name,
    source_catalog, source_schema, source_table,  # ← Keep for metadata/logging
    target_catalog, target_schema, target_table,
    source_path,  # ← NEW: Pass instead of recalculate
    spark
):
    # Remove this line:
    # source_path = f"abfss://..."  # ❌ OLD
    
    # Use passed parameter:
    print(f"Source path: {source_path}")  # ✅ NEW
    
    raw_df = (spark.readStream
        .format("cloudFiles")
        .load(source_path)  # ← Use parameter
    )
```

Apply to all 4 functions:
- `ingest_cdc_append_only_single_family()`
- `ingest_cdc_append_only_multi_family()`
- `ingest_cdc_with_merge_single_family()`
- `ingest_cdc_with_merge_multi_family()`

### 4. Update Notebook Function Calls (Cell 14)

**Before**:
```python
query = ingest_cdc_append_only_multi_family(
    storage_account_name=storage_account_name,
    container_name=container_name,
    source_catalog=source_catalog,
    source_schema=source_schema,
    source_table=source_table,
    target_catalog=target_catalog,
    target_schema=target_schema,
    target_table=target_table,
    primary_key_columns=primary_key_columns,
    spark=spark
)
```

**After**:
```python
query = ingest_cdc_append_only_multi_family(
    storage_account_name=storage_account_name,
    container_name=container_name,
    source_catalog=source_catalog,
    source_schema=source_schema,
    source_table=source_table,
    target_catalog=target_catalog,
    target_schema=target_schema,
    target_table=target_table,
    source_path=cdc_azure_path,  # ← NEW: Pass the path
    primary_key_columns=primary_key_columns,
    spark=spark
)
```

### 5. Update Helper Functions

**`check_azure_files()`** - Cell 5, line 359:

**Before**:
```python
prefix = f"parquet/{source_catalog}/{source_schema}/{source_table}/{target_table}/"  # ❌ WRONG
```

**After**:
```python
prefix = f"parquet/{source_catalog}/{source_schema}/{source_table}/"  # ✅ CORRECT
```

**Cleanup Cell** - Line 1378:

**Before**:
```python
changefeed_path = f"parquet/{source_catalog}/{source_schema}/{source_table}/{target_table}/"  # ❌ WRONG
```

**After**:
```python
changefeed_path = f"parquet/{source_catalog}/{source_schema}/{source_table}/"  # ✅ CORRECT
```

---

## Benefits

✅ **Single source of truth** - Path defined once in Cell 3
✅ **No path mismatches** - Everyone uses the same path
✅ **Easier to debug** - Path is visible in configuration
✅ **Easier to test** - Can override path for testing
✅ **Future-proof** - If CockroachDB path format changes, update once

---

## Files to Update

1. `cockroachdb-cdc-tutorial.ipynb`:
   - Cell 3: Define `cdc_relative_path` and `cdc_azure_path`
   - Cell 5: Fix `check_azure_files()` prefix
   - Cell 8: Fix changefeed path
   - Cell 14: Pass `source_path` to ingestion functions
   - Cleanup cells: Fix Azure deletion path

2. `cockroachdb_autoload.py`:
   - Add `source_path` parameter to all 4 functions
   - Remove path reconstruction logic
   - Use passed parameter

3. `cockroachdb_debug.py`:
   - Already correct (uses path without `/{target_table}`)

---

## Testing

After refactoring:

1. **Verify changefeed path**:
   ```sql
   SHOW CHANGEFEED JOBS;
   -- Check description contains correct path
   ```

2. **Check Azure files**:
   ```python
   check_azure_files(...)
   # Should show files
   ```

3. **Run ingestion**:
   ```python
   ingest_cdc_append_only_multi_family(..., source_path=cdc_azure_path, ...)
   ```

4. **Verify data**:
   ```python
   spark.read.table(target_table_fqn).count()
   # Should match source count
   ```

---

## Why This Bug Happened

1. **Historical reason**: The `/{target_table}` suffix was likely added to support multiple targets from one source
2. **CockroachDB behavior**: Ignores the suffix and writes to base path
3. **No validation**: No check that changefeed path matches Auto Loader path
4. **Path duplication**: 4+ places reconstructing the same path differently

---

## Date

2026-01-30
