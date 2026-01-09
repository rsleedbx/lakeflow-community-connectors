# Volume Path Catalog/Schema Extraction

## Issue Identified

The notebook was hardcoding `CRDB_CATALOG = "defaultdb"` and `CRDB_SCHEMA = "public"` as defaults, even though this information is **already present in the volume path structure**.

## Volume Path Structure

Unity Catalog volume paths for CockroachDB changefeeds follow a well-defined structure:

```
/Volumes/{unity_cat}/{unity_schema}/{volume}/{format}/{crdb_catalog}/{crdb_schema}/{scenario}/{timestamp?}
```

### Example Path Breakdown

```
/Volumes/main/robert_lee_cockroachdb/parquet_files/parquet/defaultdb/public/test-parquet_usertable_no_split/1767895046
         │    │                       │             │       │         │      │                                  │
         │    │                       │             │       │         │      │                                  └─ timestamp (optional)
         │    │                       │             │       │         │      └─ scenario
         │    │                       │             │       │         └─ crdb_schema ✅
         │    │                       │             │       └─ crdb_catalog ✅
         │    │                       │             └─ format
         │    │                       └─ volume_name
         │    └─ unity_schema
         └─ unity_catalog
```

## Solution: Enhanced VolumePathComponents

Added properties to `VolumePathComponents` class to extract all path components:

```python
class VolumePathComponents:
    @property
    def format_type(self) -> str:
        """Extract format type from path (e.g., 'parquet', 'json')"""
        return self._path_parts[0]
    
    @property
    def crdb_catalog(self) -> str:
        """Extract CockroachDB catalog/database name (e.g., 'defaultdb')"""
        return self._path_parts[1]
    
    @property
    def crdb_schema(self) -> str:
        """Extract CockroachDB schema name (e.g., 'public')"""
        return self._path_parts[2]
    
    @property
    def scenario(self) -> str:
        """Extract test scenario name (e.g., 'test-parquet_usertable_no_split')"""
        return self._path_parts[3]
```

## Benefits

### Before (Hardcoded)

```python
# ❌ Hardcoded - could get out of sync with path
CRDB_CATALOG = "defaultdb"
CRDB_SCHEMA = "public"

VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME_NAME}/parquet/{CRDB_CATALOG}/{CRDB_SCHEMA}/{TEST_SCENARIO}"
```

**Problems:**
- Hardcoded values could differ from actual path
- No single source of truth
- Easy to forget to update when path changes
- Requires manual maintenance

### After (Extracted from Path)

```python
# ✅ Extracted from path structure - always in sync
vol_components = parse_volume_path(VOLUME_PATH)
CRDB_CATALOG = vol_components.crdb_catalog  # 'defaultdb'
CRDB_SCHEMA = vol_components.crdb_schema    # 'public'
```

**Benefits:**
- ✅ **Single source of truth:** Path IS the configuration
- ✅ **Always in sync:** Can't get out of sync
- ✅ **Self-documenting:** Clear what comes from where
- ✅ **Reusable:** Same utility works everywhere
- ✅ **Type-safe:** Properties have docstrings and error handling

## Code Duplication Check

Checked for code duplication between:
- `sources/cockroachdb/cockroachdb.py`
- `sources/cockroachdb/scripts/changefeed_helper.py`

**Result:** ✅ **No duplication found!**

`changefeed_helper.py` already imports and uses functions from `cockroachdb.py`:

```python
from cockroachdb import (
    load_crdb_config, create_connector, analyze_azure_changefeed_files, 
    get_primary_keys, generate_test_table_sql, generate_test_insert_sql,
    generate_test_update_sql, generate_test_delete_sql, get_timestamped_path
)
```

The helper script acts as a CLI wrapper, passing arguments to the library functions. No logic is duplicated.

## Updated Notebook Flow

### Cell 6: Configuration

```python
# Parse test scenario and volume path to extract all components
from cockroachdb import parse_test_scenario, parse_volume_path

# 1. Parse scenario name (format, table, split)
scenario = parse_test_scenario(TEST_SCENARIO)
SOURCE_TABLE = scenario.table_name

# 2. Build volume path
CATALOG = pipeline_config["catalog"]
SCHEMA = pipeline_config["schema"]
VOLUME_NAME = pipeline_config["volume_name"]

VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME_NAME}/{scenario.format}/defaultdb/public/{TEST_SCENARIO}"

# 3. Parse volume path to extract CockroachDB catalog/schema
vol_components = parse_volume_path(VOLUME_PATH)
CRDB_CATALOG = vol_components.crdb_catalog  # Extracted: 'defaultdb'
CRDB_SCHEMA = vol_components.crdb_schema    # Extracted: 'public'

print(f"✅ Parsed scenario: {scenario.scenario_name}")
print(f"   Format: {scenario.format}")
print(f"   Table: {SOURCE_TABLE}")
print(f"   Split: {scenario.split_info}")
print()
print(f"✅ Parsed volume path:")
print(f"   Volume base: {vol_components.volume_base}")
print(f"   Format: {vol_components.format_type}")
print(f"   CRDB Catalog: {CRDB_CATALOG}")
print(f"   CRDB Schema: {CRDB_SCHEMA}")
print(f"   Scenario: {vol_components.scenario}")
```

## All VolumePathComponents Properties

| Property | Example Value | Source |
|----------|---------------|--------|
| `volume_base` | `/Volumes/main/robert_lee_cockroachdb/parquet_files` | First 4 path parts |
| `path_prefix` | `parquet/defaultdb/public/test-parquet_usertable_no_split` | After volume, before timestamp |
| `timestamp` | `1767895046` or `None` | Last part if 10-digit number |
| `full_path` | Complete reconstructed path | Property |
| `has_timestamp` | `True` / `False` | Property |
| `format_type` | `'parquet'` | `path_prefix` part 0 |
| `crdb_catalog` | `'defaultdb'` | `path_prefix` part 1 |
| `crdb_schema` | `'public'` | `path_prefix` part 2 |
| `scenario` | `'test-parquet_usertable_no_split'` | `path_prefix` part 3 |

## Error Handling

All properties include validation:

```python
@property
def crdb_catalog(self) -> str:
    if len(self._path_parts) < 2:
        raise ValueError(f"Path prefix '{self.path_prefix}' doesn't contain catalog")
    return self._path_parts[1]
```

This ensures:
- Clear error messages if path format is incorrect
- Fail-fast behavior
- Easy debugging

## Testing

Test with various path formats:

```python
# Standard test path
path = "/Volumes/main/robert_lee/volume/parquet/defaultdb/public/test-parquet_simple_test_no_split"
vol = parse_volume_path(path)
assert vol.format_type == 'parquet'
assert vol.crdb_catalog == 'defaultdb'
assert vol.crdb_schema == 'public'
assert vol.scenario == 'test-parquet_simple_test_no_split'

# With timestamp
path = "/Volumes/main/robert_lee/volume/json/ecommerce/staging/test-json_orders_with_split/1767895046"
vol = parse_volume_path(path)
assert vol.format_type == 'json'
assert vol.crdb_catalog == 'ecommerce'
assert vol.crdb_schema == 'staging'
assert vol.timestamp == '1767895046'
```

## Key Takeaways

1. ✅ **Path is the source of truth** - Extract from it, don't duplicate
2. ✅ **VolumePathComponents** provides clean API for accessing all path parts
3. ✅ **No code duplication** between `cockroachdb.py` and `changefeed_helper.py`
4. ✅ **Self-documenting** - Properties make intent clear
5. ✅ **Reusable utility** - Can be used in notebooks, scripts, and library code

---

**Status:** ✅ Implemented  
**Updated:** January 8, 2026  
**Files Modified:**
- `sources/cockroachdb/cockroachdb.py` (enhanced `VolumePathComponents` class)
- `sources/cockroachdb/notebooks/test_cdc_scenario.ipynb` (updated to use path extraction)
- `sources/cockroachdb/PARSE_TEST_SCENARIO_FIXES.md` (referenced)

