# JSON Analysis Bug Fix

## Problem

The `json_usertable_no_split` test was showing `upd=800` (2× the expected 400 updates), suggesting duplicate counting.

## Investigation

Using the diagnostic script `diagnose_json_double_count.py`, we confirmed:

```
✅ No duplicate UPDATE events detected
   Each key has exactly 1 UPDATE event
```

**The source data is correct!** The bug was in the analysis code.

## Root Cause

The `analyze_volume_changefeed_files()` function in `cockroachdb.py` was hardcoded to read **all files as Parquet**:

```python
# Line 3406 (OLD):
df = spark.read.parquet(file_path)
```

This caused two issues for JSON files:

1. **Wrong reader**: JSON files need `spark.read.json()`, not `spark.read.parquet()`
2. **Wrong event mapping**: JSON and Parquet have different event type semantics:
   - **Parquet**: `c` = UPSERT (could be snapshot or CDC), `d` = DELETE
   - **JSON**: `c` = SNAPSHOT, `i` = INSERT, `u` = UPDATE, `d` = DELETE

## Fix Applied

### 1. Format Detection

```python
# Detect file format
is_json = file_name.endswith(('.ndjson', '.json'))

# Read file based on format
if is_json:
    df = spark.read.json(file_path)
else:
    df = spark.read.parquet(file_path)
```

### 2. Format-Specific Event Mapping

```python
# Determine CDC operation based on format
if is_json:
    # JSON: 'c'=snapshot, 'u'=update, 'd'=delete, 'i'=insert
    if event_type == 'c':
        cdc_operation = 'SNAPSHOT'
    elif event_type == 'i':
        cdc_operation = 'INSERT'
    elif event_type == 'u':
        cdc_operation = 'UPDATE'
    elif event_type == 'd':
        cdc_operation = 'DELETE'
    else:
        cdc_operation = 'UNKNOWN'
else:
    # Parquet: 'c'=upsert (snapshot or CDC), 'd'=delete
    if event_type == 'c':
        cdc_operation = 'UPSERT'
    elif event_type == 'd':
        cdc_operation = 'DELETE'
    else:
        cdc_operation = 'UNKNOWN'
```

### 3. Updated Counting Logic

```python
elif operation == 'UPDATE' or operation == 'UPSERT':
    # UPSERT from Parquet 'c' (could be snapshot or CDC)
    # UPDATE from JSON 'u' (definitely CDC)
    total_stats['update'] += 1
    active_keys += 1
```

## Expected Results

After this fix, running `test_cdc_matrix.sh` should show:

| Test | Format | Expected upd | Previous (bug) | After Fix |
|------|--------|-------------|----------------|-----------|
| `json_usertable_no_split` | JSON | 400 | 800 (2×) | 400 ✅ |
| `json_usertable_with_split` | JSON | 400 | 800 (2×) | 400 ✅ |
| `json_simple_test_*` | JSON | 40 | 80 (2×) | 40 ✅ |

Parquet tests should be unaffected (already working correctly).

## Files Changed

- `sources/cockroachdb/cockroachdb.py`:
  - Lines 3400-3420: Added format detection and JSON reader
  - Lines 3412-3438: Format-specific event type mapping
  - Lines 3443-3468: Updated counting logic for UPSERT/UPDATE

## Testing

```bash
cd sources/cockroachdb/scripts
./test_cdc_matrix.sh
```

Look for the **COMPARISON SUMMARY** section to verify correct counts.

## Related Issues

- This bug only affected `analyze_volume_changefeed_files()`, which is used by `load_and_merge_cdc_to_delta()` for reporting
- The actual data processing and Delta table merging was unaffected
- The diagnostic script (`diagnose_json_double_count.py`) confirmed the source data integrity

## Diagnostic Script

The diagnostic script `diagnose_json_double_count.py` can be run anytime to verify JSON event integrity:

```bash
cd sources/cockroachdb/scripts
./run_json_diagnostic.sh
```

It samples JSON events and checks for duplicate UPDATE events per primary key.


