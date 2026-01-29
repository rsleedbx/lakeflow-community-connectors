# Debug Cells Refactoring Summary

## Overview
Moved inline debug cells from `cockroachdb-cdc-tutorial.ipynb` to reusable functions in `cockroachdb_debug.py`.

## Changes Made

### 1. Added Three New Functions to `cockroachdb_debug.py`

#### `quick_check_missing_keys()`
- **Purpose**: Lightweight check to see if specific keys exist in CockroachDB and staging
- **Parameters**: `conn`, `spark`, `source_table`, `target_catalog`, `target_schema`, `target_table`, `missing_keys`
- **Use Case**: Quick diagnostic when Cell 14 shows missing keys

#### `inspect_target_table()`
- **Purpose**: Comprehensive analysis of target table
- **Features**:
  - CDC operation distribution
  - Key distribution and gaps
  - Duplicate detection
  - Sample records
- **Parameters**: `spark`, `target_catalog`, `target_schema`, `target_table`
- **Use Case**: General data quality check and sync issue diagnosis

#### `detailed_missing_keys_investigation()`
- **Purpose**: Detailed investigation of missing keys
- **Features**:
  - Checks CockroachDB source
  - Checks staging table (if exists)
  - Shows CDC operation and timestamps
  - Provides detailed troubleshooting steps
- **Parameters**: `conn`, `spark`, `source_table`, `target_catalog`, `target_schema`, `target_table`, `missing_keys`
- **Use Case**: Deep dive when `inspect_target_table()` identifies specific problematic keys

### 2. Updated Notebook Debug Section

**Before**: Three large debug cells with inline code (~200+ lines total)

**After**: Three concise example cells showing how to import and use the debug functions (~20 lines each)

#### Cell 26 (Information Cell)
```python
# Debug helper functions are available in cockroachdb_debug.py
# Import them in Cell 15, then use the examples below:

print("💡 Debug functions available:")
print("   • quick_check_missing_keys() - Quick check for specific keys")
print("   • inspect_target_table() - Comprehensive target table analysis")
print("   • detailed_missing_keys_investigation() - Detailed key investigation")
print("\nSee the following cells for usage examples.")
```

#### Cell 27 (Example 1: Quick Check)
Shows how to use `quick_check_missing_keys()`

#### Cell 28 (Example 2: Target Analysis)
Shows how to use `inspect_target_table()`

#### Cell 29 (Example 3: Detailed Investigation)
Shows how to use `detailed_missing_keys_investigation()`

### 3. Updated Module Docstring

Enhanced `cockroachdb_debug.py` docstring with:
- Complete list of available functions organized by category
- Usage examples for the new functions
- Clear descriptions of when to use each function

## Benefits

1. **Reusability**: Debug functions can now be imported and used in any notebook or script
2. **Maintainability**: Debug logic is centralized in one place
3. **Cleaner Notebook**: Debug section is now concise and example-focused
4. **Better Documentation**: Module docstring provides complete API reference
5. **Easier Testing**: Functions can be unit tested independently

## Usage Pattern

```python
# In Cell 15 (or any cell where needed)
from cockroachdb_debug import (
    quick_check_missing_keys,
    inspect_target_table,
    detailed_missing_keys_investigation
)

# Use the functions with notebook variables
conn = get_cockroachdb_connection()
try:
    quick_check_missing_keys(
        conn=conn,
        spark=spark,
        source_table=source_table,
        target_catalog=target_catalog,
        target_schema=target_schema,
        target_table=target_table,
        missing_keys=[17, 18, 19]
    )
finally:
    conn.close()
```

## Files Modified

1. **`sources/cockroachdb/docs/cockroachdb_debug.py`**
   - Added 3 new functions (~180 lines)
   - Updated module docstring

2. **`sources/cockroachdb/docs/cockroachdb-cdc-tutorial.ipynb`**
   - Replaced 3 large debug cells with concise usage examples
   - Updated Cell 26 to provide debug function overview

## Validation

✅ No linting errors in `cockroachdb_debug.py`
✅ Notebook cells updated successfully
✅ All function parameters properly documented
✅ Usage examples tested for correctness
