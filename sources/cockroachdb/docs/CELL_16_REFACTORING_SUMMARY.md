# Cell 16 Diagnosis Code Refactoring

## Overview
Moved the full diagnosis setup code from Cell 16 (~120 lines) to a reusable helper function in `cockroachdb_debug.py`, and added it as Example #4 in the Debug Section.

## Changes Made

### 1. Added `run_full_diagnosis_from_config()` to `cockroachdb_debug.py`

**Purpose**: Convenience wrapper around `run_full_diagnosis()` that handles all setup from the notebook's config dictionary.

**What it does**:
- Extracts all necessary parameters from the `config` dict
- Constructs table names and Azure paths
- Refreshes target DataFrame
- Establishes CockroachDB connection
- Runs comprehensive diagnosis
- Handles cleanup (connection close)

**Signature**:
```python
def run_full_diagnosis_from_config(
    spark,
    config: Dict[str, Any],
    mismatched_columns: List[str] = None
) -> None
```

**Parameters**:
- `spark`: Spark session
- `config`: Configuration dictionary from Cell 3 with keys:
  - `cockroachdb_source`: {host, port, database, user, password, table_name}
  - `databricks_target`: {catalog, schema, table_name}
  - `azure_storage`: {storage_account_name, container_name}
  - `cdc_config`: {primary_key_columns}
- `mismatched_columns`: Optional list of columns with sum mismatches (from Cell 14)

### 2. Created Example #4 in Debug Section (Cell 30)

**Location**: After Example 3 (Cell 29) in the Debug Section

**Content**:
```python
# Example 4: Full diagnosis using config (Recommended for Cell 14 sync issues)
from cockroachdb_debug import run_full_diagnosis_from_config

# Define mismatched columns from Cell 14 output
mismatched_columns = ['field3', 'field4', 'field5', 'field6', 'field7', 'field8', 'field9']

# Run full diagnosis using config from Cell 3
run_full_diagnosis_from_config(
    spark=spark,
    config=config,
    mismatched_columns=mismatched_columns  # Set to None if no mismatches
)
```

### 3. Simplified Cell 16

**Before**: ~120 lines of setup code with imports, DataFrame refresh, connection handling, etc.

**After**: Simple informational cell that directs users to Example 4:

```python
# 🔍 Full CDC Sync Diagnosis
#
# This cell has been simplified! The full diagnosis logic is now in cockroachdb_debug.py
#
# To run full diagnosis after Cell 14 shows sync issues:
#   1. Go to the Debug Section (Cell 27-30)
#   2. Run Example 4: Full diagnosis using config

print("💡 To run full diagnosis, go to Debug Section and run Example 4 (Cell 30)")
```

## Benefits

1. **Simpler Notebook**: Cell 16 is now ~15 lines instead of ~120 lines
2. **Reusable**: The diagnosis logic can be used in any notebook or script
3. **Maintainable**: All diagnosis logic is centralized in `cockroachdb_debug.py`
4. **Better Organization**: Debug examples are now all in the Debug Section
5. **Easier to Use**: Just call one function with the config dict

## Usage Pattern

### Before (Cell 16 - 120 lines):
```python
# Import everything
import sys, os
import importlib
import cockroachdb_debug
importlib.reload(cockroachdb_debug)

# Define mismatched columns
mismatched_columns = [...]

# Get target DataFrame
target_table_fqn = f"{target_catalog}.{target_schema}.{target_table}"
target_df = spark.read.table(target_table_fqn)

# Construct paths
azure_cdc_path = f"abfss://{container_name}@..."
staging_table = f"{target_catalog}.{target_schema}.{target_table}_staging_cf"

# Print config
print("=" * 80)
...

# Establish connection
conn = get_cockroachdb_connection()

# Run diagnosis
try:
    run_full_diagnosis(...)
finally:
    conn.close()
```

### After (Example 4 - 15 lines):
```python
from cockroachdb_debug import run_full_diagnosis_from_config

mismatched_columns = ['field3', 'field4', ...]

run_full_diagnosis_from_config(
    spark=spark,
    config=config,
    mismatched_columns=mismatched_columns
)
```

## Files Modified

1. **`sources/cockroachdb/docs/cockroachdb_debug.py`**
   - Added `run_full_diagnosis_from_config()` function (~90 lines)
   - Updated module docstring to include new function

2. **`sources/cockroachdb/docs/cockroachdb-cdc-tutorial.ipynb`**
   - **Cell 16**: Simplified to informational message (~15 lines)
   - **Cell 30 (NEW)**: Added Example 4 showing full diagnosis usage
   - **Debug Section**: Now has 4 complete examples

## Migration Guide

If you have existing code that uses Cell 16:

**Option 1**: Use the new Example 4 (Recommended)
```python
from cockroachdb_debug import run_full_diagnosis_from_config
run_full_diagnosis_from_config(spark, config, mismatched_columns)
```

**Option 2**: Keep using run_full_diagnosis() directly
```python
from cockroachdb_debug import run_full_diagnosis
# Manual setup as before...
run_full_diagnosis(conn, spark, source_table, target_df, ...)
```

## Validation

✅ No linting errors in `cockroachdb_debug.py`
✅ Cell 16 simplified successfully
✅ Example 4 added to Debug Section
✅ All parameters properly documented
✅ Usage examples tested for correctness

## Next Steps

1. Run Cell 14 to check sync status
2. If mismatches found, go to Debug Section
3. Run Example 4 (Cell 30) for full diagnosis
4. Follow the troubleshooting recommendations provided
