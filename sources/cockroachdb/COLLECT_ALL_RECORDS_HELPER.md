# Iterator Pattern Helper Function: `collect_all_records()`

**Date:** January 21, 2026  
**Status:** ✅ Implemented

## Summary

Added `collect_all_records()` helper function to `cockroachdb.py` to simplify the common pattern of reading all records from a table using the iterator pattern.

## Problem

The iterator pattern required repetitive boilerplate code in notebooks and scripts:
- Manual while-loop for batch iteration
- Offset management and tracking
- Cursor comparison logic
- Statistics collection
- Safety limits for DIRECT mode

**Before (84 lines of boilerplate):**
```python
# Initialize
start_offset = {"cursor": ""}
all_records = []
batch_count = 0

# Manual while loop
while True:
    record_iterator, end_offset = connector.read_table(...)
    batch_records = list(record_iterator)
    
    if not batch_records:
        break
    
    batch_count += 1
    all_records.extend(batch_records)
    
    # ... cursor comparison, offset updates, safety limits ...

# Manual statistics
operations = {}
for record in all_records:
    op = record.get('_cdc_operation', 'UNKNOWN')
    operations[op] = operations.get(op, 0) + 1
```

## Solution

**After (20 lines with helper function):**
```python
from cockroachdb import LakeflowConnect, collect_all_records

# Initialize connector
connector = LakeflowConnect(connector_options)

# One function call!
result = collect_all_records(
    connector=connector,
    table_name='usertable',
    max_batches=100 if mode == ConnectorMode.DIRECT else None,
    debug=True
)

# Access results
all_records = result['records']
metadata = result['metadata']
print(f"Operations: {result['operations']}")
```

## Implementation Details

### Function Signature

```python
def collect_all_records(
    connector: 'LakeflowConnect',
    table_name: str,
    table_options: Dict[str, str] = None,
    max_batches: int = None,
    debug: bool = False
) -> Dict[str, Any]
```

### Return Value

```python
{
    'records': List[Dict],        # All records (deduplicated, DELETEs filtered)
    'batch_count': int,            # Number of batches read
    'operations': Dict[str, int],  # Count of each CDC operation type
    'primary_keys': List[str],     # Primary key columns
    'metadata': Dict              # Full table metadata
}
```

### Features

✅ **Automatic batch iteration** - Handles while-loop and offset management  
✅ **Statistics collection** - Tracks operations, batches, record counts  
✅ **Safety limits** - Prevents infinite loops in DIRECT mode  
✅ **Error handling** - Clear error messages for common issues  
✅ **Progress messages** - Optional debug output for monitoring  
✅ **Works with all modes** - VOLUME, AZURE_PARQUET, AZURE_JSON, DIRECT  

### Important Notes

- **Memory Warning:** All records are loaded into memory - use caution for large tables
- **Production Streaming:** For production pipelines, use `load_and_merge_cdc_to_delta()` instead
- **Deduplication:** Connector automatically applies deduplication and DELETE filtering
- **Safety Limits:** Always use `max_batches` for DIRECT mode to prevent infinite loops

## Files Modified

### 1. `cockroachdb.py` (+172 lines)

Added `collect_all_records()` function at end of file (lines 6836-7007):
- Comprehensive docstring with examples
- Automatic metadata fetching
- Batch iteration with offset management
- CDC operation statistics
- Debug output with progress tracking
- Error handling for common failures

### 2. `test_cdc_scenario.ipynb` 

**Cell 20 (Markdown):** Updated documentation
- Added description of helper function
- Listed key features and use cases
- Added note about production streaming

**Cell 25 (Python):** Simplified iterator code (84 lines → 20 lines)
- Replaced manual while-loop with `collect_all_records()`
- Removed manual statistics collection
- Added safety limit for DIRECT mode
- Cleaner, more maintainable code

## Benefits

### Code Reduction
- **76% less code** in notebooks (84 → 20 lines)
- **Single source of truth** for iterator logic
- **Consistent behavior** across all notebooks and scripts

### Developer Experience
- ✅ Easier to read and understand
- ✅ Faster to write new tests
- ✅ Less error-prone (no manual offset management)
- ✅ Built-in statistics without extra code

### Maintainability
- ✅ Changes to iterator logic only need to be made once
- ✅ Consistent error handling across all uses
- ✅ Easier to add new features (e.g., progress bars, sampling)

## Usage Examples

### Example 1: Basic Usage (VOLUME mode)

```python
from cockroachdb import LakeflowConnect, ConnectorMode, collect_all_records

connector = LakeflowConnect({
    'mode': ConnectorMode.VOLUME.value,
    'volume_path': '/Volumes/main/schema/volume/path/1234567890',
    'spark': spark,
    'dbutils': dbutils
})

result = collect_all_records(
    connector=connector,
    table_name='usertable',
    debug=True
)

print(f"✅ Read {len(result['records']):,} records")
print(f"Primary keys: {result['primary_keys']}")
print(f"CDC operations: {result['operations']}")
```

### Example 2: DIRECT mode with safety limit

```python
connector = LakeflowConnect({
    'mode': ConnectorMode.DIRECT.value,
    'connection_url': 'postgresql://...',
    'catalog': 'defaultdb',
    'schema': 'public'
})

result = collect_all_records(
    connector=connector,
    table_name='usertable',
    max_batches=100,  # Important for DIRECT mode!
    debug=True
)
```

### Example 3: Analysis and comparison

```python
# Collect records from different sources
volume_result = collect_all_records(volume_connector, 'table1', debug=True)
azure_result = collect_all_records(azure_connector, 'table1', debug=True)

# Compare
print(f"Volume:  {len(volume_result['records']):,} records")
print(f"Azure:   {len(azure_result['records']):,} records")
print(f"Match:   {len(volume_result['records']) == len(azure_result['records'])}")
```

## Testing

**Test scenarios:**
- ✅ VOLUME mode (JSON and Parquet)
- ✅ AZURE_PARQUET mode
- ✅ AZURE_JSON mode
- ✅ DIRECT mode (with safety limit)
- ✅ Empty tables
- ✅ Single batch tables
- ✅ Multi-batch tables
- ✅ Tables with column family fragmentation
- ✅ Error handling (invalid table, connection failure)

## Related Functions

This helper function complements existing automation:

| Function | Purpose | Use Case |
|----------|---------|----------|
| `collect_all_records()` | Collect all records into memory | Testing, analysis, small datasets |
| `load_and_merge_cdc_to_delta()` | Full CDC pipeline (Autoloader) | Production streaming, large datasets |
| `analyze_volume_changefeed_files()` | Analyze source files | Debugging, validation |
| `cleanup_test_checkpoint()` | Clean up test artifacts | Test teardown |

## Migration Guide

### For Existing Notebooks

**Old pattern:**
```python
# 84 lines of manual iteration...
start_offset = {"cursor": ""}
all_records = []
while True:
    # ... manual iteration ...
```

**New pattern:**
```python
# 20 lines with helper function
result = collect_all_records(
    connector=connector,
    table_name=SOURCE_TABLE,
    max_batches=100 if mode == ConnectorMode.DIRECT else None,
    debug=True
)
all_records = result['records']
metadata = result['metadata']
```

### Breaking Changes

**None** - This is a new helper function that doesn't change existing APIs.

## Future Enhancements

Potential improvements:
- [ ] Add sampling support (`max_records` parameter)
- [ ] Add progress bar for long-running iterations
- [ ] Add streaming variant that yields batches (for very large tables)
- [ ] Add schema validation option
- [ ] Add duplicate detection warnings
- [ ] Add memory usage estimation/warnings

## Documentation

- ✅ Comprehensive docstring in `cockroachdb.py`
- ✅ Updated notebook markdown documentation
- ✅ Usage examples in function docstring
- ✅ This summary document

## Conclusion

The `collect_all_records()` helper function significantly simplifies iterator pattern usage by:
1. **Reducing boilerplate** from 84 to 20 lines (76% reduction)
2. **Improving consistency** across notebooks and scripts
3. **Centralizing logic** for easier maintenance
4. **Enhancing developer experience** with built-in statistics and debug output

This follows the same philosophy as `load_and_merge_cdc_to_delta()` - wrap common patterns into reusable, well-tested functions that improve productivity and code quality.

---

**Lines of Code:**
- `cockroachdb.py`: +172 lines (new function)
- `test_cdc_scenario.ipynb`: -64 lines (simplified usage)
- **Net improvement**: Reduced application code while adding reusable library code

**Code Quality:**
- ✅ No linting errors
- ✅ Type hints included
- ✅ Comprehensive error handling
- ✅ Detailed documentation
- ✅ Production-ready
