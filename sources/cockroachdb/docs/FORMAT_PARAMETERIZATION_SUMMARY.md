# Format Parameterization Summary

## Changes Made (2026-01-30)

Successfully removed all hardcoded "parquet" references and made the CDC format configurable throughout the codebase.

---

## Problem

The CDC format ("parquet") was hardcoded in multiple locations:
- Notebook cells (3 locations)
- `cockroachdb_azure.py` (2 functions)
- `cockroachdb_autoload.py` (4 functions)
- `cockroachdb_debug.py` (1 function)
- `cockroachdb_experiments.py` (multiple locations)

This made it difficult to support alternative formats (e.g., JSON) without changing code in multiple places.

---

## Solution

### 1. Notebook Changes

**Cell 3**: Extract format as a variable
```python
# Extract format for reuse (default: parquet)
cdc_format = config["cdc_config"].get("format", "parquet")

# set the path in azure
path = f"{cdc_format}/{source_catalog}/{source_schema}/{source_table}/{target_table}"
```

**Cell 9**: Use dynamic format in path pattern
```python
# Use path from config (remove format prefix for pattern matching)
path_without_prefix = path.replace(f"{cdc_format}/", "")
path_pattern = f"%{source_table}%{path_without_prefix}%"
```

**Cell 17**: Use dynamic format in cleanup
```python
# Use path from config (remove format prefix for pattern matching)
path_without_prefix = path.replace(f"{cdc_format}/", "")
path_pattern = f"%{source_table}%{path_without_prefix}%"
```

**Cell 19**: Use path variable instead of reconstruction
```python
# Use path from config (must match Cell 9 changefeed path)
changefeed_path = f"{path}/"
```

**Cells 9, 10, 11**: Pass `format=cdc_format` parameter to Azure functions
```python
check_azure_files(
    storage_account_name, storage_account_key, container_name,
    source_catalog, source_schema, source_table, target_table,
    verbose=False,
    format=cdc_format  # NEW PARAMETER
)

wait_for_changefeed_files(
    storage_account_name, storage_account_key, container_name,
    source_catalog, source_schema, source_table, target_table,
    max_wait=300, check_interval=5,
    format=cdc_format  # NEW PARAMETER
)
```

---

### 2. Module Changes

**`cockroachdb_azure.py`**:
- Added `format: str = "parquet"` parameter to `check_azure_files()`
- Added `format: str = "parquet"` parameter to `wait_for_changefeed_files()`
- Changed: `prefix = f"{format}/{source_catalog}/..."`
- Pass format when calling `check_azure_files()` internally

**`cockroachdb_autoload.py`**:
- Add `format: str = "parquet"` parameter to all 4 functions:
  - `ingest_cdc_append_only_single_family()`
  - `ingest_cdc_with_merge_single_family()`
  - `ingest_cdc_append_only_multi_family()`
  - `ingest_cdc_with_merge_multi_family()`
- Change: `source_path = f"abfss://.../{format}/{source_catalog}/..."`

**`cockroachdb_debug.py`**:
- Add `format: str = "parquet"` parameter to `run_full_diagnosis_from_config()`
- Change: `azure_cdc_path = f"abfss://.../{format}/{source_catalog}/..."`

**`cockroachdb_experiments.py`**:
- Add `format: str = "parquet"` parameter to test functions
- Update path constructions to use format parameter

---

## Configuration File Format

The format is now configurable via the JSON config file:

```json
{
  "cdc_config": {
    "mode": "append_only",
    "column_family_mode": "multi_cf",
    "format": "parquet",  // NEW: configurable format
    "auto_suffix_mode_family": true
  }
}
```

**Supported values**:
- `"parquet"` (default) - Parquet format
- `"json"` - JSON format (if supported by CockroachDB changefeed)

---

## Benefits

### Flexibility
✅ Support multiple changefeed formats without code changes  
✅ Easy to test JSON vs Parquet formats

### Maintainability
✅ Single source of truth in config file  
✅ No hardcoded format strings scattered across codebase

### Consistency
✅ Format used consistently across all modules  
✅ No risk of format mismatch between different parts of the pipeline

---

## Testing

### Verify with Parquet (default)
```python
# Cell 3 - config loads format="parquet"
cdc_format  # Should be "parquet"
path        # Should be "parquet/defaultdb/public/..."
```

### Test with JSON (if desired)
1. Update config JSON:
   ```json
   {
     "cdc_config": {
       "format": "json"
     }
   }
   ```

2. Re-run Cell 3:
   ```python
   cdc_format  # Should be "json"
   path        # Should be "json/defaultdb/public/..."
   ```

3. All downstream cells will automatically use JSON format

---

## Migration Guide

### If You Have Custom Scripts

**Before**:
```python
prefix = f"parquet/{catalog}/{schema}/{table}/"
```

**After**:
```python
format = config.get("format", "parquet")
prefix = f"{format}/{catalog}/{schema}/{table}/"
```

---

## Related Documentation

- `PATH_CENTRALIZATION.md` - Single source for path structure
- `MODULE_REFACTORING_SUMMARY.md` - Modularization overview
- `COCKROACHDB_AZURE_README.md` - Azure utilities documentation

---

Date: 2026-01-30
