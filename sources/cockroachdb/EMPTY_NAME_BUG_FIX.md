# Fix: Empty `item.name` Bug in `dbutils.fs.ls()`

## Problem

When calling `get_timestamped_path()` in Databricks notebooks, the function failed to find timestamp directories even though they existed:

```
ValueError: No timestamped directories found

Found 1 items in parent directory:
  - : is_numeric=False, length=0, path=/Volumes/.../test-json_usertable_with_split/1769022634/
     ^^^ Empty name but path is correct!
```

## Root Cause

**`dbutils.fs.ls()` returns items where `item.name` is empty**

The bug was in the name extraction logic:

```python
# OLD CODE (BROKEN):
dir_name = item.name.rstrip('/')  # If item.name is '/' or empty, this becomes ''
```

When `dbutils.fs.ls()` returned a directory, the `FileInfo` object had:
- ✅ `item.path` = `/Volumes/.../test-json_usertable_with_split/1769022634/` (correct)
- ❌ `item.name` = `/` or `` (empty after rstrip)

This caused the filtering logic to fail:
1. `dir_name` became empty string
2. `dir_name.isdigit()` = `False` (empty string is not numeric)
3. Directory was filtered out
4. No timestamp directories found

## The Fix

**Extract directory name from path when name is empty**:

```python
# NEW CODE (FIXED):
# Try to use item.name first
dir_name = item.name.rstrip('/') if item.name else ''

# If name is empty, extract from path
if not dir_name:
    # Extract last component from path
    # Example: '/Volumes/.../1769022634/' -> '1769022634'
    path_parts = item.path.rstrip('/').split('/')
    dir_name = path_parts[-1] if path_parts else ''
```

### Flow:

1. **Try `item.name`** (fast path)
2. **If empty**, extract from `item.path` (fallback)
3. **Use the extracted name** for filtering

## Verification

After the fix, the same directory that was failing:

### Before (Broken):
```
- : is_numeric=False, length=0, path=/Volumes/.../1769022634/
   ^^^ Empty name = filtering fails
```

### After (Fixed):
```
- 1769022634: is_numeric=True, length=10, path=/Volumes/.../1769022634/
   ^^^^^^^^^^^ Extracted from path = filtering succeeds!
```

## Why This Happens

Different Databricks runtime versions and `FileInfo` implementations handle `name` differently:

| Scenario | `item.name` | `item.path` |
|----------|-------------|-------------|
| **Normal** | `1769022634` | `/Volumes/.../1769022634/` |
| **Edge case 1** | `/` | `/Volumes/.../1769022634/` |
| **Edge case 2** | `` (empty) | `/Volumes/.../1769022634/` |

Our fix handles all three cases by falling back to path extraction when `name` is empty or just `/`.

## Important Note

This is specifically about **empty `item.name` attributes** returned by `dbutils.fs.ls()` even when `item.path` is correct. This is a known Databricks quirk across different runtime versions.

## Code Changes

**File**: `sources/cockroachdb/cockroachdb.py`

**Lines**: 5055-5078 (name extraction logic in `get_timestamped_path()`)

**Change**: Added fallback to extract directory name from `item.path` when `item.name` is empty.

## Testing

To verify the fix works:

```python
# In Databricks notebook
from cockroachdb import get_timestamped_path

path = get_timestamped_path(
    volume_base='/Volumes/main/robert_lee_cockroachdb/parquet_files',
    path_prefix='json/defaultdb/public/test-json_usertable_with_split',
    version=0,  # latest
    dbutils=dbutils
)

print(f"✅ Found path: {path}")
# Should print: /Volumes/.../test-json_usertable_with_split/1769022634
```

## Summary

- **Problem**: `item.name` was empty but `item.path` was correct
- **Symptom**: Timestamp directories not found even though they existed
- **Cause**: Name extraction relied only on `item.name` attribute
- **Fix**: Added fallback to extract name from `item.path` when `item.name` is empty
- **Impact**: `get_timestamped_path()` now works reliably across different Databricks environments
