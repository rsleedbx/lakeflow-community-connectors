# Row Count Mismatch Between Iterator and Autoloader Patterns

## Problem

The notebook comparison showed a 600 row difference between patterns:
- **Autoloader Pattern**: 9,950 rows
- **Iterator Pattern**: 10,550 rows

## Root Cause

Both patterns were resolving timestamps **independently**:

1. **Autoloader** (`load_and_merge_cdc_to_delta`):
   - Called FIRST with `version=0` 
   - Resolved to latest timestamp at that time (e.g., `1769028478`)

2. **Iterator** (Community Connector):
   - Called LATER with `version=0`
   - Resolved to latest timestamp at that time (e.g., `1769034791`)

**Why different timestamps?**
- New CDC data arrived between the two calls
- CockroachDB changefeed was still running, writing new files with newer timestamps
- The `--resync` command synced new Azure data to the volume

## How Timestamp Resolution Works

Both patterns use `get_timestamped_path()` with `version=0` to find the **latest** timestamped directory:

```python
# In load_and_merge_cdc_to_delta (lines 5851-5856):
resolved_volume_path = get_timestamped_path(
    volume_base=components.volume_base,
    path_prefix=components.path_prefix,
    version=0,  # 0 = latest timestamp
    dbutils=dbutils
)
```

```python
# In Iterator configuration cell:
resolved_volume_path = get_timestamped_path(
    volume_base=components.volume_base,
    path_prefix=components.path_prefix,
    version=TEST_VERSION,  # Also 0 = latest
    dbutils=dbutils
)
```

## Diagnostic Enhancement

### Before (couldn't see actual paths):
```
VOLUME_PATH: /Volumes/.../test-scenario  # Just the prefix
```

### After (shows actual resolved paths):
```python
# load_and_merge_cdc_to_delta now returns:
result = {
    ...
    'resolved_volume_path': '/Volumes/.../test-scenario/1769028478'  # Actual path used
}
```

## Solution

### Option 1: Re-run Autoloader to Use Same Timestamp

If Iterator found newer data, simply re-run the Autoloader cell to pick up the latest timestamp:

```python
# Re-run this cell:
result = load_and_merge_cdc_to_delta(
    source_table=SOURCE_TABLE,
    volume_path=VOLUME_PATH,
    target_table_path=TARGET_TABLE_PATH,
    ...
    version=0  # Will now resolve to same timestamp as Iterator
)
```

### Option 2: Use Explicit Timestamp Path

If you want both patterns to use a specific timestamp, provide the full path:

```python
# Set explicit timestamp path
VOLUME_PATH = "/Volumes/.../test-scenario/1769034791"  # Full path with timestamp

# For Autoloader, set version=None to use exact path
result = load_and_merge_cdc_to_delta(
    ...
    volume_path=VOLUME_PATH,
    version=None  # Use exact path, no resolution
)

# For Iterator, use same full path
connector_options = {
    'mode': ConnectorMode.VOLUME,
    'volume_path': VOLUME_PATH,  # Same full path
    ...
}
```

### Option 3: Stop Changefeed During Testing

To prevent new data from arriving during testing:

```bash
# Cancel changefeed before testing
cd sources/cockroachdb/scripts
./test_cdc_matrix.sh --cancel-all

# Run notebook tests
# Both patterns will now use same timestamp

# Restart changefeed after testing
./test_cdc_matrix.sh
```

## Files Modified

### 1. `cockroachdb.py` (line 6597)
```python
# Added resolved_volume_path to return value
return {
    'success': True,
    'primary_keys': primary_keys,
    'has_column_families': has_column_families,
    'delta_count': delta_count,
    'source_count': source_count,
    'match': match,
    'query': query,
    'destination_table': target_table_path,
    'checkpoint_path': checkpoint_path,
    'resolved_volume_path': volume_path  # ✅ NEW: Show actual path used
}
```

### 2. `test_cdc_scenario.ipynb` (Cell 17)
Updated timestamp diagnostic to extract and compare actual resolved paths from both patterns:

```python
# Extract actual resolved paths from results
if 'result' in globals() and isinstance(result, dict):
    autoloader_resolved = result.get('resolved_volume_path')
    print(f"Autoloader resolved to: {autoloader_resolved}")

if 'resolved_volume_path' in globals():
    iterator_resolved = resolved_volume_path
    print(f"Iterator resolved to: {iterator_resolved}")

# Compare timestamps
autoloader_ts = autoloader_resolved.split('/')[-1]
iterator_ts = iterator_resolved.split('/')[-1]

if autoloader_ts != iterator_ts:
    print(f"⚠️  TIMESTAMPS ARE DIFFERENT!")
    print(f"   Autoloader: {autoloader_ts}")
    print(f"   Iterator: {iterator_ts}")
    print(f"\n   🔧 SOLUTION: Re-run Autoloader cell to use same timestamp")
```

## Expected Output After Fix

### If Timestamps Match:
```
================================================================================
TIMESTAMP RESOLUTION DIAGNOSTIC
================================================================================

📥 Autoloader Pattern: /Volumes/.../test-scenario/1769034791
🔄 Iterator Pattern: /Volumes/.../test-scenario/1769034791

📅 Timestamp Comparison:
   Autoloader: 1769034791
   Iterator: 1769034791

✅✅✅ PERFECT! Both patterns use the same timestamp: 1769034791
   Row count difference must be from something else (check file listing)
```

### If Timestamps Different:
```
================================================================================
TIMESTAMP RESOLUTION DIAGNOSTIC
================================================================================

📥 Autoloader Pattern: /Volumes/.../test-scenario/1769028478
🔄 Iterator Pattern: /Volumes/.../test-scenario/1769034791

📅 Timestamp Comparison:
   Autoloader: 1769028478  (older)
   Iterator: 1769034791    (newer)

⚠️  TIMESTAMPS ARE DIFFERENT!

💡 EXPLANATION:
   Both patterns resolved timestamps independently.
   Iterator picked latest (1769034791), Autoloader picked older (1769028478).
   This is likely due to new CDC data arriving after Autoloader ran.

🔧 SOLUTION:
   Re-run Autoloader cell to use the same timestamp as Iterator.
```

## Verification

After applying the fix and re-running:

1. **Check diagnostic output** - confirms both use same timestamp
2. **Check row counts** - should now match (or be very close)
3. **Check comparison output** - should show `✅✅✅ MATCH!`

## Related Issues

- **`EMPTY_ITEM_NAME_FIX.md`**: Fixed recursive directory listing (finds files in date subdirectories)
- **`RECURSIVE_DIRECTORY_SUPPORT.md`**: Enabled reading files from nested directories
- **`RESYNC_COMMAND_FEATURE.md`**: The `--resync` command can cause new timestamps to appear

## Notes

- This is **expected behavior** when changefeeds are actively running
- Not a bug - just timing-dependent resolution
- Production use cases typically don't compare patterns in real-time
- For testing, ensure stable data (stop changefeeds) or use explicit timestamp paths
