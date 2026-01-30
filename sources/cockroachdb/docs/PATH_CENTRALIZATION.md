# Path Centralization Refactoring

## Summary

Refactored the notebook to define the CDC path in only one place (Cell 3) and reuse it throughout, eliminating duplicate path construction logic.

---

## Problem

The CDC path structure was being constructed in multiple places:

### Before Refactoring

**Cell 3** (Configuration):
```python
path = f"parquet/{source_catalog}/{source_schema}/{source_table}/{target_table}"
config["cdc_config"]["path"] = path
```

**Cell 9** (Changefeed Creation):
```python
path_pattern = f"%{source_table}%{source_catalog}/{source_schema}/{source_table}/{target_table}%"
```

**Cell 17** (Cleanup - Cancel Changefeed):
```python
path_pattern = f"%{source_table}%{source_catalog}/{source_schema}/{source_table}/{target_table}%"
```

**Issues**:
- ❌ Path structure duplicated in 3 places
- ❌ Harder to maintain (change in 3 places)
- ❌ Risk of inconsistency if one location is updated but not others
- ❌ Violates DRY (Don't Repeat Yourself) principle

---

## Solution

Define the path once in Cell 3 and derive `path_pattern` from it in other cells.

### After Refactoring

**Cell 3** (Configuration) - UNCHANGED:
```python
path = f"parquet/{source_catalog}/{source_schema}/{source_table}/{target_table}"
config["cdc_config"]["path"] = path
```

**Cell 9** (Changefeed Creation) - UPDATED:
```python
# Use path from config (remove "parquet/" prefix for pattern matching)
path_without_prefix = path.replace("parquet/", "")
path_pattern = f"%{source_table}%{path_without_prefix}%"
```

**Cell 17** (Cleanup - Cancel Changefeed) - UPDATED:
```python
# Use path from config (remove "parquet/" prefix for pattern matching)
path_without_prefix = path.replace("parquet/", "")
path_pattern = f"%{source_table}%{path_without_prefix}%"
```

---

## Benefits

### Single Source of Truth
✅ Path structure defined in ONE place (Cell 3)  
✅ All other cells derive from this single definition

### Maintainability
✅ Change path structure in one place  
✅ Changes automatically propagate to all cells

### Consistency
✅ No risk of mismatch between cells  
✅ Guaranteed same path structure everywhere

### Readability
✅ Clear comment explaining the derivation  
✅ Obvious relationship between `path` and `path_pattern`

---

## Technical Details

### Path Structure

**Full Path** (used in Azure URI):
```
parquet/{source_catalog}/{source_schema}/{source_table}/{target_table}
```

Example:
```
parquet/defaultdb/public/usertable/usertable_append_only_multi_cf
```

**Path Pattern** (used in SQL LIKE query):
```
%{source_table}%{path_without_prefix}%
```

Example:
```
%usertable%defaultdb/public/usertable/usertable_append_only_multi_cf%
```

### Why Remove "parquet/" Prefix?

The CockroachDB changefeed description stored in `SHOW CHANGEFEED JOBS` contains:

```sql
CREATE CHANGEFEED FOR TABLE usertable
INTO 'azure://container/parquet/defaultdb/public/usertable/usertable_cdc?...'
WITH ...
```

The SQL `LIKE` query searches this description string. The pattern:
- `%usertable%` matches the table name
- `%defaultdb/public/usertable/usertable_cdc%` matches the path portion
- We don't include "parquet/" because the LIKE pattern needs to match the portion after the container name

---

## Usage Pattern

### When to Update Path Structure

If you need to change the path structure (e.g., add an environment prefix), update ONLY Cell 3:

```python
# Example: Add environment prefix
environment = "prod"
path = f"parquet/{environment}/{source_catalog}/{source_schema}/{source_table}/{target_table}"
config["cdc_config"]["path"] = path
```

All other cells will automatically use the new structure:
- Cell 9: Changefeed creation will check for existing changefeeds with new path
- Cell 17: Cleanup will find changefeeds with new path

---

## Related Refactoring

This path centralization complements other modularization efforts:

1. **`cockroachdb_conn.py`** - Connection management
2. **`cockroachdb_azure.py`** - Azure utilities (also uses the path structure)
3. **`cockroachdb_autoload.py`** - CDC ingestion functions
4. **Path Centralization** (this document) - Single source for path structure

See `MODULE_REFACTORING_SUMMARY.md` for complete modularization overview.

---

## Testing

### Verify Changefeed Detection Works

After refactoring, test that existing changefeed detection still works:

1. Run Cell 9 to create a changefeed
2. Run Cell 9 again - should detect existing changefeed
3. Verify output shows "Changefeed already exists"

### Verify Cleanup Works

Test that cleanup still finds and cancels the correct changefeed:

1. Run Cell 9 to create changefeed
2. Run Cell 17 to cancel it
3. Verify changefeed is cancelled
4. Check `SHOW CHANGEFEED JOBS` to confirm

---

## Future Enhancements

### Option 1: Make `path_without_prefix` a Config Variable

```python
# Cell 3
path = f"parquet/{source_catalog}/{source_schema}/{source_table}/{target_table}"
path_without_prefix = path.replace("parquet/", "")
config["cdc_config"]["path"] = path
config["cdc_config"]["path_without_prefix"] = path_without_prefix
```

Then in other cells:
```python
path_pattern = f"%{source_table}%{config['cdc_config']['path_without_prefix']}%"
```

### Option 2: Create a Helper Function

```python
# In cockroachdb_conn.py or new module
def get_changefeed_path_pattern(source_table: str, path: str) -> str:
    """Generate SQL LIKE pattern for finding changefeeds by path"""
    path_without_prefix = path.replace("parquet/", "")
    return f"%{source_table}%{path_without_prefix}%"
```

---

Date: 2026-01-30
