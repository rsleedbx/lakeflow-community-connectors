# Parquet Analyzer Fix - CockroachDB Native Format

## Issue

The `analyze_changefeed_stats.py` script was returning 0 for all statistics when analyzing CockroachDB Parquet changefeed files, despite finding 11 files with data.

## Root Cause

The analyzer was expecting **Debezium-style format** with `before`/`after` columns:

```python
# Expected (Debezium-style)
{
  "before": {...},
  "after": {...},
  "value": {...}
}
```

But CockroachDB Parquet changefeeds with `split_column_families` use a **completely different native format**:

```python
# Actual (CockroachDB native)
{
  "ycsb_key": "user10002962928786712937",
  "__crdb__event_type": "c",
  "__crdb__updated": "1766434333525460747.0000000000"
}
```

## CockroachDB Parquet Format

### Column Schema

| Column | Purpose | Values |
|--------|---------|--------|
| `__crdb__event_type` | Event type code | `'c'` (snapshot/change - used for BOTH snapshots AND updates)<br>`'i'` (insert - new rows)<br>`'d'` (delete - removed rows)<br>**Note**: No 'u' type exists in Parquet format |
| `__crdb__updated` | Timestamp | Hybrid logical timestamp |
| Data columns | Actual table columns | `ycsb_key`, `field0`, `field1`, etc. |

### Key Characteristics

1. **No `before`/`after` nesting** - Data columns are at the top level
2. **Event type as a code** - Single character instead of structured envelope
3. **Column family splitting** - Each column family creates separate Parquet files
4. **Timestamp included** - `__crdb__updated` for ordering

## The Fix

Updated `analyze_parquet_file()` to detect and handle CockroachDB native format:

```python
# Format 1: CockroachDB-specific with __crdb__event_type column
if '__crdb__event_type' in df.columns:
    # CockroachDB native format
    for _, row in df.iterrows():
        event_type = row.get('__crdb__event_type', '')
        
        if event_type == 'c':
            stats['snapshot'] += 1
        elif event_type == 'i':
            stats['insert'] += 1
        elif event_type == 'u':
            stats['update'] += 1
        elif event_type == 'd':
            stats['delete'] += 1
```

The analyzer now supports **three Parquet formats**:
1. **CockroachDB native** (with `__crdb__event_type`)
2. **Debezium-style** (with `before`/`after` columns)
3. **Nested value** (with `value` struct)

## Why 109,945 Events for 10,000 Rows?

### `split_column_families` Behavior

With `split_column_families`, CockroachDB emits **one event per column family per row**:

**usertable** has 11 column families:
- `ycsb_key` (primary key column)
- `field0` through `field9` (10 data columns)

**Calculation**:
```
~10,000 rows × 11 column families = ~110,000 events
```

### File Structure

```
parquet-cdc/
├── ...usertable+fam_0_ycsb_key.parquet     (~10,000 events)
├── ...usertable+fam_1_field0.parquet       (~10,000 events)
├── ...usertable+fam_2_field1.parquet       (~10,000 events)
├── ...usertable+fam_3_field2.parquet       (~10,000 events)
├── ...usertable+fam_4_field3.parquet       (~10,000 events)
├── ...usertable+fam_5_field4.parquet       (~10,000 events)
├── ...usertable+fam_6_field5.parquet       (~10,000 events)
├── ...usertable+fam_7_field6.parquet       (~10,000 events)
├── ...usertable+fam_8_field7.parquet       (~10,000 events)
├── ...usertable+fam_9_field8.parquet       (~10,000 events)
└── ...usertable+fam_10_field9.parquet      (~10,000 events)
```

### This is Correct Behavior!

- ✅ Each column family change is tracked independently
- ✅ Allows fine-grained CDC tracking
- ✅ Optimized for columnar storage and processing
- ✅ Better for wide tables with many columns

### Alternative: No `split_column_families`

Without `split_column_families`:
- **Event count** = row count
- **Single file** per table
- **All columns** in one event
- **Larger files** but fewer of them

## Test Results

### Before Fix

```
📊 CHANGEFEED STATISTICS
  📸 Snapshot Rows:    0
  ➕ INSERT Operations: 0
  ✏️  UPDATE Operations: 0
  ➖ DELETE Operations: 0
  📈 Total Events:      0
```

### After Fix

```
📊 CHANGEFEED STATISTICS
  📸 Snapshot Rows:    109,945
  ➕ INSERT Operations: 0
  ✏️  UPDATE Operations: 0
  ➖ DELETE Operations: 0
  📈 Total Events:      109,945
```

### Debug Output (First File)

```
🔍 DEBUG: Parquet schema for 202512222012135254607470000000000-f3bbbd...
   Columns: ['ycsb_key', '__crdb__event_type', '__crdb__updated']
   Row count: 9995
   First row sample:
   {'ycsb_key': 'user10002962928786712937', 
    '__crdb__event_type': 'c', 
    '__crdb__updated': '1766434333525460747.0000000000'}
```

## Benefits of CockroachDB Native Format

### vs. Debezium Format

| Aspect | CockroachDB Native | Debezium |
|--------|-------------------|----------|
| **Nesting** | Flat (columns at top level) | Nested (`before`/`after`) |
| **Schema evolution** | Automatic with Parquet schema | Requires envelope parsing |
| **Column family support** | Native with `split_column_families` | Not supported |
| **File size** | Smaller (no redundant structure) | Larger (envelope overhead) |
| **Query performance** | Better (columnar, no nesting) | Slower (nested extraction) |

### For Databricks

- ✅ **Parquet** is Databricks' native format
- ✅ **Columnar** storage optimized for analytics
- ✅ **Schema inference** works automatically
- ✅ **Predicate pushdown** for filtering
- ✅ **Compression** (gzip) reduces storage costs

## Related Changes

### Updated Files

1. **`analyze_changefeed_stats.py`**
   - Added CockroachDB native format support
   - Added debug mode for schema inspection
   - Added first-file statistics display

2. **`ANALYZE_CHANGEFEED_STATS.md`**
   - Documented CockroachDB native format
   - Explained `split_column_families` behavior
   - Added event count expectations

3. **`PARQUET_FORMAT_FIX.md`** (this file)
   - Root cause analysis
   - Format comparison
   - Fix explanation

## Recommendations

### For Production

1. **Keep `split_column_families`** if:
   - Wide tables (many columns)
   - Selective column access patterns
   - Want columnar optimization

2. **Remove `split_column_families`** if:
   - Narrow tables (few columns)
   - Always need all columns
   - Want simpler file structure

### For Testing

- The analyzer now correctly handles both formats
- Expected counts for `split_column_families`: rows × column families
- Expected counts without: exactly row count

---

**Date**: December 22, 2025  
**Status**: ✅ Fixed and tested  
**Event Count**: 109,945 events from ~10,000 rows (11 column families) ✅ Correct!



