# Noisy Debug Output Fix - JSON Column Family Fragments

## Problem

When running `test_cdc_matrix.sh` with JSON changefeeds using `split_column_families=true` (like `test_json_usertable_with_split`), the debug output was flooded with thousands of lines:

```
Skipping fragment without complete PK: .../test_json_usertable_with_split+data-1.ndjson (has 0/1 PK columns)
Skipping fragment without complete PK: .../test_json_usertable_with_split+data-1.ndjson (has 0/1 PK columns)
... (repeated thousands of times)
```

This made the test output extremely noisy and hard to read.

## Root Cause

**Location**: `cockroachdb.py` lines 3252-3253 in `analyze_azure_changefeed_files()`

For JSON changefeeds with `split_column_families=true`, CockroachDB emits multiple JSON records per logical row:
- One "primary" record containing the primary key (e.g., `ycsb_key`)
- Separate records for each column family (e.g., `+data-1`, `+data-2`) **without the primary key**

The analysis code correctly skips these column family fragments (since we only want to count each logical row once), but it was printing a debug message **for every single fragment row**.

For a table with 10,000 rows and 10 fields in the `data` family, this would print 10,000 skip messages!

## Fix Applied

**Changed**: Print a **summary** of skipped fragments instead of per-row messages

### Before (Lines 3252-3254):
```python
if len(cdc_key_pairs) < len(primary_key_columns):
    if debug:
        print(f"   Skipping fragment without complete PK: {blob_name} (has {len(cdc_key_pairs)}/{len(primary_key_columns)} PK columns)")
    continue
```

### After (Lines 3209-3277):
```python
skipped_fragments = {}  # Track skipped fragments per file

for blob_name in data_blobs:
    file_skipped = 0
    # ... processing logic ...
    
    if len(cdc_key_pairs) < len(primary_key_columns):
        file_skipped += 1  # Count instead of print
        continue
    
    # Track files with skipped fragments
    if file_skipped > 0:
        skipped_fragments[blob_name] = file_skipped

# Print summary at the end
if skipped_fragments and debug:
    total_skipped = sum(skipped_fragments.values())
    print(f"   ℹ️  Skipped {total_skipped:,} column family fragments without PK from {len(skipped_fragments)} files (expected for split_column_families=true)")
```

## Expected Results

### Before (Noisy):
```
Skipping fragment without complete PK: file1.ndjson (has 0/1 PK columns)
Skipping fragment without complete PK: file1.ndjson (has 0/1 PK columns)
... (10,000 lines) ...
Skipping fragment without complete PK: file19.ndjson (has 0/1 PK columns)
```

### After (Clean):
```
   ℹ️  Skipped 9,594 column family fragments without PK from 19 files (expected for split_column_families=true)
```

## Behavior

- **For JSON with `split_column_families=false`**: No change (no fragments to skip)
- **For JSON with `split_column_families=true`**: Clean summary output instead of thousands of lines
- **For Parquet**: No change (Parquet fragments always have PKs)

## Why This is Correct

Skipping column family fragments without complete PKs is **intentional and correct** for JSON changefeeds:

1. Each logical row is split into multiple JSON records (one per family)
2. Only the "primary" record has the primary key
3. Column family fragments (e.g., `+data-1`) don't have the PK
4. We count each logical row exactly once by only processing the PK-containing fragment
5. This is different from Parquet where ALL fragments contain the PK

The fix maintains the correct counting logic while reducing output noise by ~10,000×.

## Impact

- ✅ Test output is now readable
- ✅ Correct counting behavior preserved
- ✅ Debug information still available (summary format)
- ✅ Users can see if column family splitting is working correctly

## Files Changed

- `cockroachdb.py` (lines 3209-3277): Changed `analyze_azure_changefeed_files()` to track and summarize skipped fragments instead of printing each one

## Testing

Run any JSON test with column families:
```bash
cd sources/cockroachdb/scripts
./test_cdc_matrix.sh
```

Look for the `json_usertable_with_split` test - it should now show clean output with a single summary line for skipped fragments.

## Related Documentation

- `JSON_ANALYSIS_BUG_FIX.md`: How JSON vs Parquet formats are handled differently
- `SOURCE_COUNT_ZERO_FIX.md`: Debug parameter passing and error visibility


