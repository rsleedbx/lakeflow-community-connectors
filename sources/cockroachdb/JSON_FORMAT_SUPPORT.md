# JSON Format Support in load_and_merge_cdc_to_delta

## Problem

The `load_and_merge_cdc_to_delta` function was hardcoded to only process Parquet files, even though:
- ✅ The connector supports creating JSON changefeeds
- ✅ The test matrix validates both JSON and Parquet
- ✅ The analysis functions handle both formats
- ❌ The notebook function couldn't load JSON scenarios

## Root Cause

Three hardcoded references to Parquet:

1. **Autoloader configuration** (line ~5401):
   ```python
   .option("cloudFiles.format", "parquet")  # ❌ HARDCODED
   ```

2. **Path glob filter** (line ~5406):
   ```python
   .option("pathGlobFilter", f"*{effective_table}*.parquet")  # ❌ HARDCODED
   ```

3. **File listing** (line ~966):
   ```python
   if file_info.name.endswith('.parquet'):  # ❌ HARDCODED
   ```

## Solution Implemented

### 1. Auto-detect Format from Volume Path

Use the existing `VolumePathComponents.format_type` property:

```python
# Extract format from volume path
try:
    file_format = components.format_type  # 'parquet' or 'json'
    if debug:
        print(f"🔍 Auto-detected format: {file_format}")
except:
    # Fallback: assume parquet if can't detect
    file_format = 'parquet'

# Set file extension and cloudFiles format based on detected format
if file_format == 'json':
    file_extension = '.ndjson'
    cloudfiles_format = 'json'
else:
    file_extension = '.parquet'
    cloudfiles_format = 'parquet'
```

### 2. Dynamic Autoloader Configuration

```python
df_raw = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", cloudfiles_format)  # ✅ DYNAMIC
    .option("pathGlobFilter", f"*{effective_table}*{file_extension}")  # ✅ DYNAMIC
    .load(volume_path)
)
```

### 3. Enhanced File Listing

Updated `_list_volume_files` to support multiple extensions:

```python
def _list_volume_files(
    self, 
    volume_path: str = None, 
    spark = None, 
    dbutils = None, 
    file_extensions: List[str] = None  # ✅ NEW PARAMETER
) -> List[Dict[str, Any]]:
    # Default to both Parquet and JSON files
    if file_extensions is None:
        file_extensions = ['.parquet', '.ndjson', '.json']
    
    # Check if file matches any of the desired extensions
    if any(file_info.name.endswith(ext) for ext in file_extensions):
        file_list.append({...})
```

## Volume Path Structure

The format is explicitly part of the path structure:

```
/Volumes/{unity_cat}/{unity_schema}/{volume}/{format}/{catalog}/{schema}/{scenario}/{timestamp?}
                                              ^^^^^^
                                              parquet or json
```

**Examples:**
```
# JSON scenario
/Volumes/main/robert_lee/volume/json/defaultdb/public/test-json_usertable_with_split/1767895046/

# Parquet scenario
/Volumes/main/robert_lee/volume/parquet/defaultdb/public/test-parquet_simple_test_no_split/1767895046/
```

## Benefits

### Before (Parquet Only)
```python
# ❌ Only these scenarios worked:
TEST_SCENARIO = "test-parquet_usertable_no_split"
TEST_SCENARIO = "test-parquet_usertable_with_split"
TEST_SCENARIO = "test-parquet_simple_test_no_split"
TEST_SCENARIO = "test-parquet_simple_test_with_split"

# ❌ These scenarios failed:
TEST_SCENARIO = "test-json_usertable_no_split"     # FileNotFoundError: no .parquet files
TEST_SCENARIO = "test-json_usertable_with_split"   # FileNotFoundError: no .parquet files
TEST_SCENARIO = "test-json_simple_test_no_split"   # FileNotFoundError: no .parquet files
TEST_SCENARIO = "test-json_simple_test_with_split" # FileNotFoundError: no .parquet files
```

### After (Full Support)
```python
# ✅ ALL 8 scenarios now work:
TEST_SCENARIO = "test-json_usertable_with_split"     # ✅ Auto-detects JSON
TEST_SCENARIO = "test-json_usertable_no_split"       # ✅ Auto-detects JSON
TEST_SCENARIO = "test-json_simple_test_with_split"   # ✅ Auto-detects JSON
TEST_SCENARIO = "test-json_simple_test_no_split"     # ✅ Auto-detects JSON
TEST_SCENARIO = "test-parquet_usertable_with_split"  # ✅ Auto-detects Parquet
TEST_SCENARIO = "test-parquet_usertable_no_split"    # ✅ Auto-detects Parquet
TEST_SCENARIO = "test-parquet_simple_test_with_split"# ✅ Auto-detects Parquet
TEST_SCENARIO = "test-parquet_simple_test_no_split"  # ✅ Auto-detects Parquet
```

## Testing

### Test All Scenarios

```python
# In test_cdc_scenario.ipynb, simply change TEST_SCENARIO:

# JSON with column families (merging test)
TEST_SCENARIO = "test-json_usertable_with_split"

# JSON without column families  
TEST_SCENARIO = "test-json_simple_test_no_split"

# Parquet with column families
TEST_SCENARIO = "test-parquet_usertable_with_split"

# Function auto-detects format and uses correct:
# - File extension (.ndjson vs .parquet)
# - Autoloader format (json vs parquet)
# - Schema inference rules
```

### Debug Output

When debug=True, shows format detection:

```
🔍 Auto-detected format: json
🔍 Validating volume path...
   File format: json
   File extension: .ndjson
   ✅ Found 12 json file(s)

📥 Loading data with Autoloader (json format)...
   ✅ Autoloader configured
```

## Format-Specific Handling

### JSON (.ndjson)
- **Schema columns:** `before`, `after`, `key`, `updated`, `_rescued_data`
- **Envelope format:** `before` and `after` are **structs** containing row data
- **CDC detection:**
  - `after` only + `updated` <= cutoff → `SNAPSHOT`
  - `after` only + `updated` > cutoff → `INSERT`
  - `after` + `before` → `UPDATE`
  - `before` only → `DELETE`
- **Column families:** Fragments have `before`/`after` without PK, uses top-level `key` field
- **Data access:** Function flattens `after` struct to top-level columns automatically

### Parquet (.parquet)
- **Schema columns:** Data columns at top level + `__crdb__event_type`, `__crdb__updated`
- **Flat format:** Data columns directly accessible (no nesting)
- **Event types:** `c` (create/update), `i` (insert), `d` (delete)
- **CDC detection:**
  - `__crdb__event_type='c'` + `__crdb__updated` <= cutoff → `SNAPSHOT`
  - `__crdb__event_type='c'` + `__crdb__updated` > cutoff → `UPDATE`
  - `__crdb__event_type='i'` → `INSERT`
  - `__crdb__event_type='d'` → `DELETE`
- **Column families:** Fragments per column family (e.g., `+pk`, `+data`)

### Automatic Format Detection

The `_add_cdc_metadata_to_dataframe` function automatically detects format from schema:

```python
# Detect format from DataFrame schema
schema_columns = df.columns
is_json_format = 'before' in schema_columns and 'after' in schema_columns
is_parquet_format = '__crdb__event_type' in schema_columns

if is_json_format:
    # Flatten 'after' struct to top level
    # Use 'before'/'after' for CDC detection
    # Timestamp column: 'updated'
    ...
elif is_parquet_format:
    # Data already at top level
    # Use '__crdb__event_type' for CDC detection
    # Timestamp column: '__crdb__updated'
    ...
```

## Backward Compatibility

✅ **Fully backward compatible:**
- Existing Parquet-only paths still work
- Default file extensions include both formats
- Fallback to Parquet if format can't be detected
- No breaking changes to function signatures

## Files Modified

1. **cockroachdb.py** (lines 5183-5412):
   - Added format auto-detection from volume path
   - Made Autoloader configuration dynamic
   - Enhanced `_list_volume_files` to support multiple extensions
   - Updated validation messages

2. **cockroachdb.py** (lines 1167-1330):
   - Enhanced `_add_cdc_metadata_to_dataframe` to detect and handle both formats
   - JSON: Uses `before`/`after` envelope, flattens `after` struct to top level
   - Parquet: Uses `__crdb__event_type` column
   - Automatic format detection from DataFrame schema

3. **__init__.py**:
   - Exported `parse_volume_path`, `VolumePathComponents`, `parse_test_scenario`
   - Enables format detection utilities in notebooks

## Related Features

This enhancement complements existing JSON support:

| Feature | Status | Notes |
|---------|--------|-------|
| Create JSON changefeed | ✅ Working | `_create_json_changefeed()` |
| Analyze JSON files | ✅ Working | `analyze_azure_changefeed_files()` |
| Test matrix validation | ✅ Working | `test_cdc_matrix.sh` |
| Notebook streaming | ✅ **NEW** | `load_and_merge_cdc_to_delta()` |
| DLT integration | ✅ Working | Custom DLT tables |

## Next Steps

Users can now:
1. ✅ Test all 8 scenarios in `test_cdc_scenario.ipynb`
2. ✅ Compare JSON vs Parquet performance
3. ✅ Validate column family merging works identically
4. ✅ Use either format in production pipelines

---

**Status:** ✅ Implemented  
**Date:** January 8, 2026  
**Impact:** All 8 test scenarios now work in notebooks  
**Breaking Changes:** None  

