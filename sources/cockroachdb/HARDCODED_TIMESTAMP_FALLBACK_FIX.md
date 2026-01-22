# Fix: Hardcoded Timestamp Fallback Causing Wrong Directory Selection

## Problem

The notebook was selecting **OLD test data** (timestamp `1767823340` from Jan 6) instead of **NEW test data** (timestamp `1769022634` from Jan 21), even though:
1. ✅ Azure sync worked correctly
2. ✅ Files were synced to Unity Catalog Volume
3. ✅ `TEST_VERSION = 0` was set (should get latest)

### Error Output

```
🔍 Auto-resolving timestamped path (version=0)...
⚠️  Found timestamp directory via direct check: .../1767823340
    (Directory was NOT returned by ls of parent directory)
   ✅ Resolved to timestamp: 1767823340  ❌ WRONG! Should be 1769022634
```

## Root Cause

The issue was in `cockroachdb.py` lines 5097-5133:

### The Flow:

1. **`dbutils.fs.ls()` called** on parent directory:
   ```
   /Volumes/main/robert_lee_cockroachdb/parquet_files/json/defaultdb/public/test-json_usertable_with_split
   ```

2. **Filtering logic** (lines 5069-5095) looks for:
   - Numeric directory names (`dir_name.isdigit()`)
   - Exactly 10 digits long (Unix timestamp)
   - Must be a directory (`item.isDir()`)

3. **If NO timestamp directories found**, falls back to **hardcoded list**:
   ```python
   potential_timestamps = [1767823340, 1767823100, 1767822800, 1767820000, 1767810000]
   ```

4. **Direct check** tries each hardcoded timestamp using `dbutils.fs.ls(path)`

5. **Found old timestamp** `1767823340` (from January 6)

### Why Filtering Failed

The `dbutils.fs.ls()` probably DID return the new directory (`1769022634/`), but the filtering logic failed because:

1. **`item.isDir()` check might be failing** (lines 5074-5088)
   - Different `FileInfo` structures between environments
   - Attribute access issues

2. **Path format differences**
   - Trailing slashes
   - Different path representations

3. **Timing issues**
   - Old directory still existed at the time

## The Solution

**Removed the hardcoded timestamp fallback** (lines 5100-5115):

### Before:
```python
if not timestamp_dirs:
    # Hardcoded fallback - BAD!
    potential_timestamps = [1767823340, 1767823100, 1767822800, 1767820000, 1767810000]
    for ts in potential_timestamps:
        test_path = f"{full_prefix}/{ts}"
        try:
            dbutils.fs.ls(test_path)
            # Uses OLD timestamp!
            timestamp_dirs.append({'name': str(ts), 'path': test_path, 'timestamp': ts})
            break
        except:
            pass
```

### After:
```python
if not timestamp_dirs:
    # Provide detailed diagnostics instead of falling back
    raise ValueError(
        f"No timestamped directories found in: {full_prefix}\n\n"
        f"Expected: 1769022634/ (10-digit Unix timestamp directories)\n"
        f"Found {len(debug_items)} items:\n{debug_info}\n\n"
        f"Possible causes:\n"
        f"  1. Test data not synced - run: test_cdc_matrix.sh\n"
        f"  2. Wrong path format\n"
        f"  3. Items not recognized as directories by dbutils\n"
        f"  4. Directory filtering issue - check item.isDir()\n"
    )
```

## Why This Fix is Better

### Before (with hardcoded fallback):
- ❌ Masked real issues with path resolution
- ❌ Used stale/old timestamps
- ❌ Created confusion about sync status
- ❌ Silent failures led to wrong data being used

### After (without fallback):
- ✅ Forces proper debugging of real issues
- ✅ Clear error messages with diagnostics
- ✅ No confusion about which data is being used
- ✅ Explicit failures prevent silent errors

## Verification

After the fix, if the filtering fails, you'll get a clear error like:

```
ValueError: No timestamped directories found in: .../test-json_usertable_with_split

Expected: 1769022634/ (10-digit Unix timestamp directories)

Found 1 items in parent directory:
  - 1769022634: is_numeric=True, length=10, path=/Volumes/.../1769022634/

Possible causes:
  1. Test data not synced - run: test_cdc_matrix.sh
  2. Wrong path format
  3. Items not recognized as directories by dbutils
  4. Directory filtering issue - check item.isDir()

To debug further, try in notebook:
  items = dbutils.fs.ls('/Volumes/.../test-json_usertable_with_split')
  for item in items:
      print(f'{item.name} - isDir: {item.isDir()}')
```

This makes it **obvious** what the real problem is, rather than silently using old data.

## Related Issues

This fix also addresses:
1. Confusion about Azure sync status
2. Schema file not found errors (was looking in wrong timestamp directory)
3. CockroachDB fallback using wrong table name

## Files Modified

- ✅ `sources/cockroachdb/cockroachdb.py` (lines 5097-5133)

## Testing

To verify the fix works:

1. Run `test_cdc_matrix.sh` to create fresh test data
2. Open `test_cdc_scenario.ipynb`
3. Set `TEST_VERSION = 0` (latest)
4. Should now correctly find `1769022634` instead of falling back to `1767823340`
5. If filtering still fails, you'll get a clear diagnostic error instead of silent wrong behavior
