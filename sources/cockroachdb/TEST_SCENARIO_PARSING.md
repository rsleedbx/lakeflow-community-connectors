# Test Scenario Parsing Utility

## Overview

The `parse_test_scenario()` function and `TestScenarioComponents` class provide centralized parsing of test scenario names used in `test_cdc_matrix.sh` and notebooks, eliminating code duplication.

## Test Scenario Naming Convention

Test scenarios follow this pattern:
```
test-{format}_{table_name}_{split_info}
```

Where:
- **format**: `parquet` or `json`
- **table_name**: Can be multi-word (e.g., `simple_test`, `usertable`)
- **split_info**: `with_split` or `no_split`

### Examples

- `test-parquet_simple_test_no_split`
  - format: `parquet`
  - table: `simple_test`
  - split: `no_split` (has_split=False)

- `test-json_usertable_with_split`
  - format: `json`
  - table: `usertable`
  - split: `with_split` (has_split=True)

## Usage

### Basic Example

```python
from cockroachdb import parse_test_scenario

# Parse a scenario
scenario = parse_test_scenario('test-parquet_simple_test_no_split')

# Access components
print(scenario.format)      # 'parquet'
print(scenario.table_name)  # 'simple_test'
print(scenario.has_split)   # False
print(scenario.split_info)  # 'no_split' (property)
print(scenario.catalog)     # 'defaultdb' (default)
print(scenario.schema)      # 'public' (default)
```

### Understanding Catalog/Schema

```python
# Scenario name ONLY contains: format, table, split
scenario = parse_test_scenario('test-parquet_orders_no_split')

# Catalog/schema come from configuration or defaults
CRDB_CATALOG = "ecommerce"  # From config or default
CRDB_SCHEMA = "staging"     # From config or default

# Volume path shows where catalog/schema actually live:
# /Volumes/{unity_catalog}/{unity_schema}/{volume}/parquet/{crdb_catalog}/{crdb_schema}/{scenario}
# Example: /Volumes/main/robert_lee/parquet_files/parquet/ecommerce/staging/test-parquet_orders_no_split
```

### Notebook Integration

Before (inline parsing):
```python
# Old way - duplicated logic
scenario_parts = TEST_SCENARIO.split('_')
if len(scenario_parts) >= 4:
    SOURCE_TABLE = '_'.join(scenario_parts[1:-2])
else:
    raise ValueError(f"Invalid format: {TEST_SCENARIO}")

CRDB_CATALOG = "defaultdb"
CRDB_SCHEMA = "public"
```

After (using utility):
```python
# New way - centralized, reusable
from cockroachdb import parse_test_scenario

scenario = parse_test_scenario(TEST_SCENARIO)

# Extract table name from scenario
SOURCE_TABLE = scenario.table_name

# Catalog/schema are NOT in scenario name - use defaults or config
CRDB_CATALOG = "defaultdb"  # Default or from config
CRDB_SCHEMA = "public"      # Default or from config

# Additional info available
print(f"Format: {scenario.format}")
print(f"Split: {scenario.split_info}")
```

## Class Reference

### `TestScenarioComponents`

**Attributes:**
- `scenario_name` (str): Original scenario name
- `format` (str): Data format ('parquet' or 'json')
- `table_name` (str): Extracted table name
- `has_split` (bool): Whether column families are split

**Properties:**
- `split_info` (str): Returns 'with_split' or 'no_split' based on `has_split`

**Note:** Catalog/schema are NOT part of the scenario name. They come from:
- Defaults (`defaultdb`/`public`)
- Configuration files
- Volume path structure (`/Volumes/.../format/catalog/schema/...`)

**Methods:**
- `__repr__()`: Full representation
- `__str__()`: Returns scenario_name

### `parse_test_scenario()`

**Signature:**
```python
def parse_test_scenario(scenario_name: str) -> TestScenarioComponents
```

**Parameters:**
- `scenario_name`: Test scenario name (e.g., 'test-parquet_simple_test_no_split')

**Returns:**
- `TestScenarioComponents` object with parsed components

**Raises:**
- `ValueError`: If scenario name format is invalid

## Benefits

✅ **No Code Duplication:** Centralized parsing logic  
✅ **Type Safety:** Explicit class attributes instead of magic strings  
✅ **Validation:** Built-in format validation with helpful error messages  
✅ **Maintainability:** Single source of truth for parsing logic  
✅ **Readability:** Clear, self-documenting code  

## Error Handling

Invalid formats raise descriptive `ValueError`:

```python
try:
    scenario = parse_test_scenario('invalid-format')
except ValueError as e:
    print(e)
    # Output:
    # Invalid test scenario format: 'invalid-format'
    # Expected: test-{format}_{table}_{split_info}
    # Examples:
    #   - test-parquet_simple_test_no_split
    #   - test-json_usertable_with_split
```

## Related Utilities

- `parse_volume_path()`: Parses Unity Catalog volume paths
- `VolumePathComponents`: Container for volume path components
- `get_timestamped_path()`: Resolves timestamped test directories

## Implementation

See `cockroachdb.py`:
- Class: `TestScenarioComponents` (lines ~4095-4140)
- Function: `parse_test_scenario()` (lines ~4140-4300)

## Testing

All 8 test scenarios are valid:
- ✅ `test-json_usertable_with_split`
- ✅ `test-json_usertable_no_split`
- ✅ `test-json_simple_test_with_split`
- ✅ `test-json_simple_test_no_split`
- ✅ `test-parquet_usertable_with_split`
- ✅ `test-parquet_usertable_no_split`
- ✅ `test-parquet_simple_test_with_split`
- ✅ `test-parquet_simple_test_no_split`

---

**Status:** ✅ Implemented  
**Updated:** January 8, 2026  
**Files Modified:**
- `sources/cockroachdb/cockroachdb.py` (added class and function)
- `sources/cockroachdb/notebooks/test_cdc_scenario.ipynb` (using new utility)

