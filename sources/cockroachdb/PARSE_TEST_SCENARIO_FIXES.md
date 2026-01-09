# Parse Test Scenario Fixes

## Issues Identified

1. ❌ **Deleted scenario list**: The cell with `TEST_SCENARIO` definitions and available scenarios was accidentally deleted
2. ❌ **Catalog/schema confusion**: `parse_test_scenario()` was incorrectly including `catalog` and `schema` parameters, which are NOT part of the scenario name

## Root Cause

**Catalog/Schema are NOT in the scenario name!**

The scenario name format is:
```
test-{format}_{table_name}_{split_info}
```

Examples:
- `test-parquet_simple_test_no_split` → format, table, split ONLY
- `test-json_usertable_with_split` → format, table, split ONLY

Catalog and schema come from:
- **Defaults:** `defaultdb`/`public`
- **Configuration files:** `cockroachdb_pipelines.json`
- **Volume path structure:** `/Volumes/{unity_cat}/{unity_schema}/{volume}/parquet/{crdb_catalog}/{crdb_schema}/{scenario}/`

## Fixes Applied

### 1. Fixed `parse_test_scenario()` Function

**Before (incorrect):**
```python
def parse_test_scenario(
    scenario_name: str,
    catalog: str = 'defaultdb',
    schema: str = 'public'
) -> TestScenarioComponents:
    # ...
    return TestScenarioComponents(
        scenario_name=scenario_name,
        format=format_str,
        table_name=table_name,
        has_split=has_split,
        catalog=catalog,  # ❌ NOT part of scenario name!
        schema=schema     # ❌ NOT part of scenario name!
    )
```

**After (correct):**
```python
def parse_test_scenario(scenario_name: str) -> TestScenarioComponents:
    # ...
    return TestScenarioComponents(
        scenario_name=scenario_name,
        format=format_str,
        table_name=table_name,
        has_split=has_split
        # ✅ No catalog/schema - they're not in the scenario name
    )
```

### 2. Fixed `TestScenarioComponents` Class

**Before (incorrect):**
```python
class TestScenarioComponents:
    def __init__(self, scenario_name, format, table_name, has_split, 
                 catalog='defaultdb', schema='public'):
        self.catalog = catalog
        self.schema = schema
```

**After (correct):**
```python
class TestScenarioComponents:
    def __init__(self, scenario_name, format, table_name, has_split):
        # ✅ No catalog/schema attributes
        pass
```

### 3. Restored Notebook Configuration Cell

**Restored cell with:**
- ✅ `TEST_SCENARIO` variable definition
- ✅ `TEST_VERSION` variable definition
- ✅ List of all 8 available test scenarios with comments
- ✅ Clear indication of which scenario to change

**Cell content:**
```python
# ============================================================================
# 🔧 CHANGE THIS TO TEST DIFFERENT SCENARIOS
# ============================================================================

# Test scenario (subdirectory name from test_cdc_matrix.sh)
TEST_SCENARIO = "test-parquet_usertable_no_split"  # ⭐ Change this!

# Test version (which test run to analyze)
TEST_VERSION = 0  # 0=latest, 1=second newest, -1=oldest

# ============================================================================
# Available test scenarios (JSON first, then Parquet):
#   - "test-json_usertable_with_split"        ⭐ Tests merge with column families
#   - "test-json_usertable_no_split"
#   - "test-json_simple_test_with_split"
#   - "test-json_simple_test_no_split"
#   - "test-parquet_usertable_with_split"
#   - "test-parquet_usertable_no_split"
#   - "test-parquet_simple_test_with_split"
#   - "test-parquet_simple_test_no_split"
# ============================================================================
```

### 4. Fixed Notebook Parsing Cell

**Updated to:**
```python
from cockroachdb import parse_test_scenario

scenario = parse_test_scenario(TEST_SCENARIO)

# Extract table name from scenario
SOURCE_TABLE = scenario.table_name

# CockroachDB connection defaults (used for schema auto-detection)
# These are NOT part of the scenario name - they're configuration
CRDB_CATALOG = "defaultdb"  # CockroachDB database/catalog
CRDB_SCHEMA = "public"      # CockroachDB schema

print(f"✅ Parsed scenario: {scenario.scenario_name}")
print(f"   Format: {scenario.format}")
print(f"   Table: {SOURCE_TABLE}")
print(f"   Split: {scenario.split_info}")
print(f"   Using catalog: {CRDB_CATALOG}")
print(f"   Using schema: {CRDB_SCHEMA}")
```

## Volume Path Structure Explained

The volume path clearly shows that catalog/schema are separate from the scenario:

```
/Volumes/{unity_catalog}/{unity_schema}/{volume_name}/parquet/{crdb_catalog}/{crdb_schema}/{test_scenario}
         └─────────────────────────────────┘        └────────────────────────┘ └──────────────┘
         Unity Catalog structure                    CockroachDB location       Scenario name
         (main/robert_lee_cockroachdb)              (defaultdb/public)         (test-parquet_...)
```

### Example Path Breakdown

```
/Volumes/main/robert_lee_cockroachdb/parquet_files/parquet/defaultdb/public/test-parquet_usertable_no_split/1767895046
         │    │                       │             │       │         │      │                                  │
         │    │                       │             │       │         │      │                                  └─ Timestamp
         │    │                       │             │       │         │      └─ TEST_SCENARIO (format + table + split)
         │    │                       │             │       │         └─ CRDB_SCHEMA (from config)
         │    │                       │             │       └─ CRDB_CATALOG (from config)
         │    │                       │             └─ Format directory
         │    │                       └─ Volume name (from config)
         │    └─ Unity schema (from config)
         └─ Unity catalog (from config)
```

## Key Takeaways

✅ **Scenario name ONLY contains:** format, table name, split info  
✅ **Catalog/schema come from:** Configuration or defaults  
✅ **Volume path contains:** Both Unity Catalog location AND CockroachDB catalog/schema  
✅ **Notebook now has:** Clear scenario list with all 8 options  

## Benefits of Fix

1. **Correct Semantics:** Function doesn't parse things that aren't in the input
2. **Clear Documentation:** Comments explain where catalog/schema come from
3. **No Confusion:** Scenario name vs configuration are clearly separated
4. **Maintainability:** If catalog/schema change, no need to update scenario names

## Testing

All 8 scenarios parse correctly:
```python
# All return correct format, table_name, has_split (no catalog/schema)
parse_test_scenario('test-json_usertable_with_split')
parse_test_scenario('test-json_usertable_no_split')
parse_test_scenario('test-json_simple_test_with_split')
parse_test_scenario('test-json_simple_test_no_split')
parse_test_scenario('test-parquet_usertable_with_split')
parse_test_scenario('test-parquet_usertable_no_split')
parse_test_scenario('test-parquet_simple_test_with_split')
parse_test_scenario('test-parquet_simple_test_no_split')
```

---

**Status:** ✅ Fixed  
**Updated:** January 8, 2026  
**Files Modified:**
- `sources/cockroachdb/cockroachdb.py` (fixed function and class)
- `sources/cockroachdb/notebooks/test_cdc_scenario.ipynb` (restored scenario list, fixed parsing)
- `sources/cockroachdb/TEST_SCENARIO_PARSING.md` (updated documentation)

