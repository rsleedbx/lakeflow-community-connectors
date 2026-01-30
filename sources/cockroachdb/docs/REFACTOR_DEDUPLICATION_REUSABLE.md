# Refactoring: Reusable Deduplication Functions

## Overview

Extracted deduplication logic from inline code into reusable functions in `cockroachdb_ycsb.py`. This ensures consistency across Cell 14 verification and debug functions.

## Problem

Deduplication logic for append_only mode was duplicated in multiple places:
1. Cell 14 (verification cell) - inline code
2. `find_mismatched_rows()` in cockroachdb_debug.py - inline code
3. `compare_row_by_row()` in cockroachdb_debug.py - inline code

Each implementation had to:
- Detect timestamp column (`_cdc_timestamp` or `__crdb__updated`)
- Create window partitioned by primary keys
- Order by timestamp descending
- Keep only latest row per key

**Risk**: Code duplication → potential inconsistencies if one implementation is updated but others aren't.

## Solution: Reusable Functions

### 1. `deduplicate_to_latest()`

Core deduplication function that takes any DataFrame and returns deduplicated version.

**Location**: `cockroachdb_ycsb.py`

**Signature**:
```python
def deduplicate_to_latest(
    df,
    primary_keys: List[str],
    timestamp_col: str = None,  # Auto-detects if None
    verbose: bool = False
) -> DataFrame
```

**Features**:
- Auto-detects timestamp column (_cdc_timestamp or __crdb__updated)
- Optional verbose mode for progress messages
- Returns deduplicated DataFrame with latest row per key

**Example**:
```python
# Simple usage
target_df_latest = deduplicate_to_latest(
    target_df, 
    primary_keys=['ycsb_key'],
    verbose=True
)

# Custom timestamp column
target_df_latest = deduplicate_to_latest(
    target_df,
    primary_keys=['order_id', 'line_id'],
    timestamp_col='event_time'
)
```

### 2. `get_column_sum_spark_deduplicated()`

Convenience function that deduplicates first, then calculates sum.

**Location**: `cockroachdb_ycsb.py`

**Signature**:
```python
def get_column_sum_spark_deduplicated(
    df,
    column_name: str,
    primary_keys: List[str],
    timestamp_col: str = None,
    verbose: bool = False
) -> int
```

**Features**:
- Combines deduplication + sum calculation in one call
- Perfect for Cell 14 verification where you need sums after deduplication

**Example**:
```python
# For append_only mode verification
if cdc_mode == "append_only":
    # This automatically deduplicates before summing
    target_sum = get_column_sum_spark_deduplicated(
        target_df,
        'field0',
        primary_keys=['ycsb_key'],
        verbose=True
    )
else:
    # For update_delete mode, no deduplication needed
    target_sum = get_column_sum_spark(target_df, 'field0')
```

## Changes Made

### 1. Added Functions to `cockroachdb_ycsb.py`

Added two new functions after `get_column_sum_spark()`:
- `deduplicate_to_latest()` - Core deduplication
- `get_column_sum_spark_deduplicated()` - Deduplicate + sum

### 2. Updated Cell 6 (Imports)

**Before**:
```python
from cockroachdb_ycsb import (
    get_table_stats,
    get_table_stats_spark,
    get_column_sum,
    get_column_sum_spark
)
```

**After**:
```python
from cockroachdb_ycsb import (
    get_table_stats,
    get_table_stats_spark,
    get_column_sum,
    get_column_sum_spark,
    deduplicate_to_latest,              # NEW
    get_column_sum_spark_deduplicated   # NEW
)
```

### 3. Updated Cell 14 (Verification)

**Before** (28 lines of inline code):
```python
if cdc_mode == "append_only":
    from pyspark.sql.window import Window
    from pyspark.sql import functions as F
    
    print("\n📝 Mode: APPEND_ONLY - Deduplicating to latest state per key...")
    
    timestamp_col = "_cdc_timestamp" if "_cdc_timestamp" in target_df.columns else "__crdb__updated"
    window_spec = Window.partitionBy(*primary_key_columns).orderBy(F.col(timestamp_col).desc())
    
    target_df_deduplicated = target_df.withColumn("_row_num", F.row_number().over(window_spec)) \
                                     .filter(F.col("_row_num") == 1) \
                                     .drop("_row_num")
    
    deduplicated_count = target_df_deduplicated.count()
    original_count = target_df.count()
    print(f"   ✅ Deduplicated: {original_count} rows → {deduplicated_count} rows")
    
    target_df = target_df_deduplicated
```

**After** (2 lines):
```python
if cdc_mode == "append_only":
    print("\n📝 Mode: APPEND_ONLY - Deduplicating to latest state per key...")
    target_df = deduplicate_to_latest(target_df, primary_key_columns, verbose=True)
```

### 4. Updated `cockroachdb_debug.py`

**Function**: `find_mismatched_rows()`

**Before** (15 lines):
```python
timestamp_col = '_cdc_timestamp' if '_cdc_timestamp' in target_df.columns else '__crdb__updated'

print(f"   Deduplicating using {timestamp_col} (keeping latest row per key)...")

window_spec = Window.partitionBy(*primary_keys).orderBy(F.col(timestamp_col).desc())

target_df_with_rank = target_df.withColumn("_row_num", F.row_number().over(window_spec))
target_df_latest = target_df_with_rank.filter(F.col("_row_num") == 1).drop("_row_num")

target_df_filtered = target_df_latest.select(*target_columns)

print(f"   ✅ Deduplicated target to latest state per key")
```

**After** (5 lines):
```python
from cockroachdb_ycsb import deduplicate_to_latest

target_df_latest = deduplicate_to_latest(target_df, primary_keys, verbose=True)

target_df_filtered = target_df_latest.select(*target_columns)
```

**Also removed**: `from pyspark.sql.window import Window` (no longer needed)

## Benefits

### 1. **Consistency**
All deduplication uses identical logic → identical results everywhere

### 2. **Maintainability**
Update once in `cockroachdb_ycsb.py` → all callers benefit

### 3. **Readability**
```python
# Before: 28 lines of complex PySpark code
# After:  1 line function call
target_df = deduplicate_to_latest(target_df, primary_key_columns, verbose=True)
```

### 4. **Reusability**
Can be used anywhere:
- Cell 14 verification
- Debug functions
- Custom analysis notebooks
- Ad-hoc queries

### 5. **Testability**
Single function to test → easier to validate correctness

## Usage Examples

### Example 1: Cell 14 Verification (Append-Only)

```python
# Load target
target_df = spark.read.table(target_table_fqn)

# Deduplicate for append_only mode
if cdc_mode == "append_only":
    target_df = deduplicate_to_latest(target_df, primary_key_columns, verbose=True)

# Now compare sums (both will match)
source_sum = get_column_sum(conn, source_table, 'field0')
target_sum = get_column_sum_spark(target_df, 'field0')

if source_sum == target_sum:
    print("✅ Sums match!")
```

### Example 2: Alternative Using Combined Function

```python
# Even simpler - deduplicate + sum in one call
if cdc_mode == "append_only":
    target_sum = get_column_sum_spark_deduplicated(
        target_df, 'field0', 
        primary_keys=['ycsb_key'],
        verbose=True
    )
else:
    target_sum = get_column_sum_spark(target_df, 'field0')
```

### Example 3: Debug Function

```python
# In cockroachdb_debug.py
from cockroachdb_ycsb import deduplicate_to_latest

def analyze_target_data(target_df, primary_keys, is_append_only):
    if is_append_only:
        target_df = deduplicate_to_latest(target_df, primary_keys, verbose=True)
    
    # Continue with analysis on deduplicated data
    ...
```

## Testing

To verify the refactoring:

1. **Run Cell 14** → Should show identical results before/after refactoring
2. **Run diagnosis (Cell 30)** → Should show identical results before/after refactoring  
3. **Check consistency** → Cell 14 and diagnosis should now always agree

## Files Changed

1. `/sources/cockroachdb/docs/cockroachdb_ycsb.py`
   - Added `deduplicate_to_latest()`
   - Added `get_column_sum_spark_deduplicated()`

2. `/sources/cockroachdb/docs/cockroachdb-cdc-tutorial.ipynb`
   - Cell 6: Added imports
   - Cell 14: Replaced inline code with function call

3. `/sources/cockroachdb/docs/cockroachdb_debug.py`
   - `find_mismatched_rows()`: Use reusable function
   - Removed `Window` import (no longer needed)

## Migration Path

For existing notebooks:

```python
# Old way (inline deduplication)
from pyspark.sql.window import Window
window_spec = Window.partitionBy('key').orderBy(F.col('_cdc_timestamp').desc())
df_dedup = df.withColumn("_row_num", F.row_number().over(window_spec)) \
           .filter(F.col("_row_num") == 1) \
           .drop("_row_num")

# New way (reusable function)
from cockroachdb_ycsb import deduplicate_to_latest
df_dedup = deduplicate_to_latest(df, primary_keys=['key'], verbose=True)
```

## Future Enhancements

Potential additions to the function:

1. **Support for custom sort columns**:
   ```python
   deduplicate_to_latest(df, ['key'], order_by=['timestamp', 'sequence_num'])
   ```

2. **Keep first instead of last**:
   ```python
   deduplicate_to_latest(df, ['key'], keep='first')  # Instead of 'last'
   ```

3. **Multiple timestamp fallbacks**:
   ```python
   deduplicate_to_latest(df, ['key'], timestamp_cols=['_cdc_timestamp', '__crdb__updated', 'event_time'])
   ```
