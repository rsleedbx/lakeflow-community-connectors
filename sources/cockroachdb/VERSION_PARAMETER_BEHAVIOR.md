# Version Parameter Behavior Change

## Summary

Changed the `version` parameter behavior in `load_and_merge_cdc_to_delta()` to make timestamp resolution explicit and intuitive.

**Date:** January 8, 2026  
**Status:** ✅ Implemented

## Old Behavior (Confusing)

Previously, the function tried to auto-detect timestamps in the path:
- If timestamp found in path → use it
- If no timestamp → try to resolve using `version` parameter
- Fallback to legacy format if resolution fails

This was confusing because:
- Hard to know when timestamp resolution would happen
- Implicit behavior based on path structure
- Difficult to use exact paths when needed

## New Behavior (Explicit)

Now the `version` parameter **explicitly controls** timestamp resolution:

### `version=None` (Default for Explicit Paths)
- **Use exact path as provided**
- No timestamp resolution
- Works for both:
  - Explicit paths with timestamp: `path/scenario/1767895046/`
  - Legacy paths without timestamp: `path/scenario/`

### `version=0` (or any int) - Auto-Resolve Timestamp
- **Automatically finds and appends timestamp directory**
- `0` = latest timestamp
- `1` = second newest  
- `-1` = oldest
- **Recommended for test_cdc_matrix.sh data**

## Usage Examples

### Example 1: Auto-Resolve Latest Test Data (Recommended)

```python
result = load_and_merge_cdc_to_delta(
    source_table='simple_test',
    volume_path='dbfs:/Volumes/main/schema/volume/json/defaultdb/public/test-json_simple_test_no_split',
    target_table_path='main.schema.simple_test_delta',
    spark=spark,
    dbutils=dbutils,
    crdb_config=crdb_config,
    version=0  # Auto-find latest: finds test-json_simple_test_no_split/1767895046/
)
```

**Output:**
```
📂 Parsed volume path:
   Volume base: dbfs:/Volumes/main/schema/volume
   Path prefix: json/defaultdb/public/test-json_simple_test_no_split
   Timestamp in path: None
   Version parameter: 0
🔍 Auto-resolving timestamped path (version=0)...
   ✅ Resolved to timestamp: 1767895046
   Full path: dbfs:/Volumes/.../test-json_simple_test_no_split/1767895046
```

### Example 2: Explicit Path with Timestamp

```python
result = load_and_merge_cdc_to_delta(
    source_table='simple_test',
    volume_path='dbfs:/Volumes/main/schema/volume/json/defaultdb/public/test-json_simple_test_no_split/1767895046',
    target_table_path='main.schema.simple_test_delta',
    spark=spark,
    dbutils=dbutils,
    crdb_config=crdb_config,
    version=None  # Use exact path (no resolution)
)
```

**Output:**
```
📂 Parsed volume path:
   Volume base: dbfs:/Volumes/main/schema/volume
   Path prefix: json/defaultdb/public/test-json_simple_test_no_split
   Timestamp in path: 1767895046
   Version parameter: None
✅ Using explicit path with timestamp: 1767895046
```

### Example 3: Legacy Data (No Timestamp)

```python
result = load_and_merge_cdc_to_delta(
    source_table='simple_test',
    volume_path='dbfs:/Volumes/main/schema/volume/old_data/simple_test',
    target_table_path='main.schema.simple_test_delta',
    spark=spark,
    dbutils=dbutils,
    crdb_config=crdb_config,
    version=None  # Use exact path (legacy format)
)
```

**Output:**
```
📂 Parsed volume path:
   Volume base: dbfs:/Volumes/main/schema/volume
   Path prefix: old_data/simple_test
   Timestamp in path: None
   Version parameter: None
✅ Using explicit path (no timestamp)
   💡 To use latest test data, set version=0
```

## Migration Guide

### For Notebooks Using test_cdc_matrix.sh Data

**Before (implicit):**
```python
VOLUME_PATH = f"dbfs:/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/json/.../test-scenario"
version = 0  # Works but behavior unclear
```

**After (explicit):**
```python
VOLUME_PATH = f"dbfs:/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/json/.../test-scenario"
version = 0  # Explicitly auto-resolves to latest timestamp ✅
```

**No changes needed!** Just clearer what's happening.

### For Notebooks Using Legacy Data

**Before (implicit):**
```python
VOLUME_PATH = "dbfs:/Volumes/.../old_data/scenario"
version = 0  # Would fail, then fallback to legacy
```

**After (explicit):**
```python
VOLUME_PATH = "dbfs:/Volumes/.../old_data/scenario"
version = None  # Explicitly use exact path ✅
```

### For Notebooks Using Explicit Timestamp Paths

**Before (implicit):**
```python
VOLUME_PATH = "dbfs:/Volumes/.../scenario/1767895046"
# version not needed, auto-detected from path
```

**After (explicit):**
```python
VOLUME_PATH = "dbfs:/Volumes/.../scenario/1767895046"
version = None  # Explicitly use exact path ✅
```

## Benefits

1. **Explicit Control** - No guessing about timestamp resolution
2. **Clearer Intent** - Code clearly shows what behavior is expected
3. **Better Errors** - Errors tell you exactly what to change
4. **No Fallbacks** - Fails fast with clear messages instead of silent fallbacks
5. **Consistent** - Same behavior every time for same inputs

## Default Values

### In load_and_merge_cdc_to_delta()
```python
def load_and_merge_cdc_to_delta(
    ...,
    version: int = None  # Default: use exact path
)
```

**Why `None` as default?**
- **Principle of least surprise** - does exactly what path says
- **Explicit over implicit** - must opt-in to auto-resolution
- **Backward compatible** - works with all path formats

### In test_cdc_scenario.ipynb
```python
TEST_VERSION = 0  # Recommended for test_cdc_matrix.sh data
```

**Why `0` in notebook?**
- Notebook specifically designed for test_cdc_matrix.sh data
- Users expect latest test data by default
- Can easily change to `None` for legacy data

## Error Messages

### When version=0 but no timestamps found:
```
Failed to auto-resolve timestamped path:
No timestamped directories found in: dbfs:/Volumes/.../test-scenario

Options:
1. Set version=None to use exact path: dbfs:/Volumes/.../test-scenario
2. Run test_cdc_matrix.sh to generate timestamped test data
3. Provide full path with timestamp in volume_path parameter
```

Clear actionable guidance!

## Testing

Validated with:
- ✅ Auto-resolution with version=0
- ✅ Explicit path with version=None
- ✅ Legacy data with version=None
- ✅ All 8 test scenarios from validation
- ✅ Clear error messages

## Documentation Updated

- ✅ Function docstring with examples
- ✅ Parameter description
- ✅ Path format documentation
- ✅ This migration guide

## Related Files

- `sources/cockroachdb/cockroachdb.py` - Main implementation
- `sources/cockroachdb/notebooks/test_cdc_scenario.ipynb` - Example usage
- `sources/cockroachdb/VERSION_PARAMETER_INTEGRATION.md` - Original integration docs

