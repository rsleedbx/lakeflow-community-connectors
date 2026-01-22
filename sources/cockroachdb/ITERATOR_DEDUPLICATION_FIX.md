# Iterator Pattern Deduplication Fix

**Date**: 2026-01-21  
**Issue**: Iterator pattern produced 10,450 rows while Autoloader produced 9,950 rows  
**Root Cause**: Iterator was not deduplicating CDC events by primary key, resulting in multiple events (SNAPSHOT + UPDATE) for the same key being counted as separate rows.

## Problem Analysis

### Test Scenario
- **Initial snapshot**: 10,000 rows
- **INSERT operations**: 50 new rows
- **UPDATE operations**: 400 rows (modifying existing keys)
- **DELETE operations**: 100 rows

**Expected final count**: 10,000 + 50 - 100 = **9,950 rows**

### What Iterator Was Doing (WRONG)
```
Raw records: 20,700 (column family fragments)
↓ merge_column_family_fragments()
After merge: 10,550 (grouped by PK + timestamp + operation)
↓ filter out DELETEs
Final: 10,450 rows ❌
```

**Problem**: Kept separate rows for SNAPSHOT and UPDATE of the same key.

### What Autoloader Does (CORRECT)
```
Raw records: 20,700 (column family fragments)
↓ merge_column_family_fragments()
After merge: 10,550 (grouped by PK + timestamp + operation)
↓ deduplicate by PK only (keep latest by timestamp)
After dedup: 10,050 (removed 500 duplicate events)
↓ filter out DELETEs
Final: 9,950 rows ✅
```

**Key insight**: Autoloader keeps only the **latest state per primary key**, not separate rows for each operation type.

## Changes Made

### 1. Fixed `merge_column_family_fragments()` (Lines 5480-5603)

**Added `_cdc_updated` to metadata columns**:
```python
metadata_columns = [
    '__crdb__event_type', '__crdb__updated', '_rescued_data',
    '_cdc_operation', '_cdc_timestamp', '_cdc_updated', '_source_file', '_processing_time',  # Added _cdc_updated
    '_metadata',
    'after', 'before', 'key', 'updated',
    '_after_json', '_before_json', '_debug_after_first_10', '_debug_before_first_10'
]
```

**Updated timestamp column detection** (2 locations):
```python
# For fragmentation detection (line ~5527)
if '_cdc_timestamp' in all_columns:
    timestamp_col_for_check = '_cdc_timestamp'
elif '_cdc_updated' in all_columns:  # Added this check
    timestamp_col_for_check = '_cdc_updated'
elif '__crdb__updated' in all_columns:
    timestamp_col_for_check = '__crdb__updated'
elif 'updated' in all_columns:
    timestamp_col_for_check = 'updated'

# For grouping (line ~5595)
if '_cdc_timestamp' in all_columns:
    timestamp_col = '_cdc_timestamp'
elif '_cdc_updated' in all_columns:  # Added this check
    timestamp_col = '_cdc_updated'
elif '__crdb__updated' in all_columns:
    timestamp_col = '__crdb__updated'
elif 'updated' in all_columns:
    timestamp_col = 'updated'
```

### 2. Added Deduplication Logic to `_read_table_from_volume()` (Lines 1407-1456)

**New logic added before returning data**:
```python
# Apply CDC deduplication (keep latest state per key, like Autoloader)
if all_rows and primary_keys:
    try:
        # Convert to pandas DataFrame for merge/dedup operations
        import pandas as pd
        pdf = pd.DataFrame(all_rows)
        
        # Convert to Spark for proper merge handling
        df_raw = spark.createDataFrame(pdf)
        
        # Merge column family fragments (if table uses split column families)
        df_merged = merge_column_family_fragments(
            df_raw,
            primary_key_columns=primary_keys,
            debug=False
        )
        
        # Deduplicate: Keep only LATEST state per primary key
        from pyspark.sql import Window
        from pyspark.sql.functions import row_number, col
        
        # Determine which timestamp column to use
        timestamp_col = None
        if '_cdc_timestamp' in df_merged.columns:
            timestamp_col = '_cdc_timestamp'
        elif '_cdc_updated' in df_merged.columns:
            timestamp_col = '_cdc_updated'
        elif '__crdb__updated' in df_merged.columns:
            timestamp_col = '__crdb__updated'
        elif 'updated' in df_merged.columns:
            timestamp_col = 'updated'
        
        if timestamp_col:
            # Deduplicate by PK only, keeping latest by timestamp
            window_spec = Window.partitionBy(*primary_keys).orderBy(col(timestamp_col).desc())
            df_deduped = df_merged.withColumn("_row_num", row_number().over(window_spec)) \
                                  .filter("_row_num == 1") \
                                  .drop("_row_num")
            
            # Filter out DELETE operations
            df_final = df_deduped.filter("_cdc_operation != 'DELETE'")
            
            # Convert back to list of dicts
            pdf_final = df_final.toPandas()
            all_rows = pdf_final.to_dict('records')
    except Exception as e:
        # If deduplication fails, return raw data
        print(f"⚠️  Deduplication failed: {e}")
        print(f"   Returning raw data without deduplication")
```

## Testing Results

### Notebook Output (test_cdc_scenario.ipynb)
```
🔧 Checking for column family fragmentation...
   Total records: 20,700
   Primary keys: ['ycsb_key']

🔧 Renamed _cdc_updated → _cdc_timestamp for merge compatibility

📊 Fragmentation Detection:
   Total rows: 20,700
   Unique events (PK + timestamp + operation): 10,550
   Duplication ratio: 2.0x

🔧 Column family fragmentation detected!
   Merging 20,700 fragments into 10,550 distinct CDC events...

✅ Merge transformation applied!
   Batch DataFrame merged

📊 After merge: 10550 rows
📊 Columns: 14

🔧 Applying CDC operations (keeping latest state per key)...
   Removed 500 duplicate events (SNAPSHOT+UPDATE for same keys)
   After deduplication: 10,050 rows

🔧 Filtering out DELETE operations...
   Removed 100 DELETE records
   Final row count: 9,950

💾 Writing iterator data to Delta table...
✅ Successfully wrote 9,950 rows
```

**Result**: ✅ **9,950 rows** - matches Autoloader exactly!

## Impact

### Before
- Iterator: 10,450 rows ❌
- Autoloader: 9,950 rows ✅
- **Mismatch**: 500 extra rows

### After
- Iterator: 9,950 rows ✅
- Autoloader: 9,950 rows ✅
- **Match**: Perfect alignment

## Files Modified

1. **`sources/cockroachdb/cockroachdb.py`**
   - `merge_column_family_fragments()`: Added `_cdc_updated` support
   - `_read_table_from_volume()`: Added deduplication logic

## Next Steps

1. ✅ Test with the notebook to verify 9,950 row count
2. ⏳ Apply same deduplication logic to Azure iterator methods:
   - `_read_table_from_azure_parquet()`
   - `_read_table_from_azure_json()`
3. ⏳ Test with different table schemas and CDC patterns
4. ⏳ Consider performance optimization for large datasets

## Technical Notes

### Why Deduplication is Needed

CockroachDB changefeeds emit multiple events for the same key:
1. **SNAPSHOT** event during initial scan
2. **UPDATE** events for subsequent changes to the same key

For the final Delta table state, we only want **one row per primary key** representing the latest state. Autoloader handles this automatically via Delta merge logic. The Iterator pattern now matches this behavior.

### Why `_cdc_updated` was Missing

The `_cdc_updated` column is added by the `_process_json_records()` and `_process_parquet_records()` methods when transforming raw CDC records. It was not included in the `metadata_columns` list in `merge_column_family_fragments()`, causing it to be treated as a data column and dropped during the merge aggregation.

### Comparison to Autoloader Logic

This implementation mirrors the logic in `load_and_merge_cdc_to_delta()` (lines 6448-6458):
```python
# CRITICAL: For initial table creation, keep only LATEST state per key
window_spec = Window.partitionBy(*primary_keys).orderBy(F.col("_cdc_timestamp").desc())
final_rows = (rows_after_delete
    .withColumn("_row_num", F.row_number().over(window_spec))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num")
)
```
