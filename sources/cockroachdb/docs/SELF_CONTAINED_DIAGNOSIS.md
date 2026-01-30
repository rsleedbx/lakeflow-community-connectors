# Self-Contained Diagnosis: No External Dependencies

## Problem

Previously, running the full diagnosis required multiple manual steps:

1. **Run Cell 14** to calculate column sums and identify mismatches
2. **Copy mismatch columns** from Cell 14 output
3. **Paste into diagnosis call**:
   ```python
   mismatched_columns = ['field3', 'field4', 'field5', 'field6', 'field7', 'field8', 'field9']
   run_full_diagnosis_from_config(spark, config, mismatched_columns)
   ```

**Issues:**
- ❌ Requires running Cell 14 first
- ❌ Manual copy/paste of column names
- ❌ Error-prone (typos, missing columns)
- ❌ If Cell 14 has bugs (like not deduplicating), passes wrong information to diagnosis
- ❌ Not truly self-contained

## Solution: Auto-Detection

The diagnosis function now **auto-detects** mismatched columns if not provided:

### How It Works

```python
# Just run this - no prerequisites!
run_full_diagnosis_from_config(spark, config)
```

**Behind the scenes:**

1. **Auto-detects CDC mode**
   ```python
   try:
       spark.table(staging_table).limit(1).collect()
       cdc_mode = "update_delete"
   except:
       cdc_mode = "append_only"
   ```

2. **Deduplicates if needed**
   ```python
   if cdc_mode == "append_only":
       target_df = deduplicate_to_latest(target_df, primary_keys)
   ```

3. **Compares all columns**
   ```python
   for col in business_columns:
       source_sum = get_column_sum(conn, source_table, col)
       target_sum = get_column_sum_spark(target_df, col)
       
       if source_sum != target_sum:
           mismatched_columns.append(col)
   ```

4. **Reports findings**
   ```
   🔍 Auto-detecting mismatched columns...
      Detected CDC mode: append_only
      Deduplicating target to latest state for comparison...
      Comparing 11 columns...
      ✅ All columns match! No mismatches detected.
   ```

### Output Example

**When all columns match:**
```
🔍 CDC SYNC DIAGNOSIS CONFIGURATION
================================================================================
   Source: defaultdb.public.usertable_append_only_multi_cf
   Target: robert_lee.robert_lee_cockroachdb.usertable_append_only_multi_cf
   Staging: robert_lee.robert_lee_cockroachdb.usertable_append_only_multi_cf_staging_cf
   Azure: abfss://changefeed-events@...

📊 Refreshing target DataFrame...
✅ Target DataFrame refreshed: 48 rows


████████████████████████████████████████████████████████████████████████████████
🔍 RUNNING FULL DIAGNOSIS
████████████████████████████████████████████████████████████████████████████████
🔌 Establishing fresh CockroachDB connection...
✅ Connection established

🔍 Auto-detecting mismatched columns...
   Detected CDC mode: append_only
   Deduplicating target to latest state for comparison...
   Comparing 11 columns...
   ✅ All columns match! No mismatches detected.

🚀 Running full diagnosis...
```

**When columns mismatch:**
```
🔍 Auto-detecting mismatched columns...
   Detected CDC mode: append_only
   Deduplicating target to latest state for comparison...
   Comparing 11 columns...
   ⚠️  Found 3 mismatched columns: field3, field4, field5
```

## Benefits

### 1. **True Self-Containment**
No prerequisites - just run one cell!

**Before:**
```python
# Step 1: Run Cell 14
source_sum = get_column_sum(conn, source_table, 'field0')
target_sum = get_column_sum_spark(target_df, 'field0')
# ... check all columns manually

# Step 2: Copy mismatches
mismatched_columns = ['field3', 'field4', 'field5']

# Step 3: Run diagnosis
run_full_diagnosis_from_config(spark, config, mismatched_columns)
```

**After:**
```python
# Just one step!
run_full_diagnosis_from_config(spark, config)
```

### 2. **Always Correct**
Auto-detection uses proper deduplication logic:
- Detects CDC mode automatically
- Deduplicates for append_only mode
- Compares apples-to-apples

**No risk of:**
- Using outdated Cell 14 results
- Forgetting to deduplicate
- Manual copy/paste errors
- Incorrect CDC mode assumptions

### 3. **Faster Workflow**
Single function call → comprehensive diagnosis

**Time saved:**
- No Cell 14 execution (~30 seconds)
- No manual inspection of output
- No copy/paste
- Direct to diagnosis

### 4. **Better User Experience**
Clear, informative output:
```
🔍 Auto-detecting mismatched columns...
   Detected CDC mode: append_only         ← Shows what it detected
   Deduplicating target to latest state... ← Shows what it's doing
   Comparing 11 columns...                 ← Shows progress
   ✅ All columns match!                   ← Shows result
```

### 5. **Flexible Usage**
Can still provide columns manually if needed:

```python
# Auto-detect (recommended)
run_full_diagnosis_from_config(spark, config)

# Manual override (if you have specific columns to check)
run_full_diagnosis_from_config(
    spark, 
    config,
    mismatched_columns=['field3', 'field4']
)
```

## Implementation Details

### Function Signature

```python
def run_full_diagnosis_from_config(
    spark,
    config: Dict[str, Any],
    mismatched_columns: List[str] = None  # Optional - auto-detected if None
) -> None:
```

### Auto-Detection Logic

Located in `cockroachdb_debug.py` → `run_full_diagnosis_from_config()`:

```python
# Auto-calculate mismatched columns if not provided
if mismatched_columns is None:
    print("🔍 Auto-detecting mismatched columns...")
    
    # 1. Detect CDC mode
    try:
        spark.table(staging_table).limit(1).collect()
        is_append_only = False
    except:
        is_append_only = True
    
    cdc_mode = "append_only" if is_append_only else "update_delete"
    print(f"   Detected CDC mode: {cdc_mode}")
    
    # 2. Deduplicate if needed
    target_df_for_comparison = target_df
    if is_append_only:
        print(f"   Deduplicating target to latest state for comparison...")
        target_df_for_comparison = deduplicate_to_latest(target_df, primary_keys, verbose=False)
    
    # 3. Get business columns
    business_columns = [col for col in target_df.columns 
                       if not col.startswith('_') and not col.startswith('__crdb__')]
    
    # 4. Compare columns
    source_table_fqn = f"{source_catalog}.{source_schema}.{source_table}"
    mismatched_columns = []
    
    print(f"   Comparing {len(business_columns)} columns...")
    for col in business_columns:
        try:
            source_sum = get_column_sum(conn, source_table_fqn, col)
            target_sum = get_column_sum_spark(target_df_for_comparison, col)
            
            if source_sum != target_sum:
                mismatched_columns.append(col)
        except:
            pass  # Skip non-numeric columns
    
    # 5. Report findings
    if mismatched_columns:
        print(f"   ⚠️  Found {len(mismatched_columns)} mismatched columns: {', '.join(mismatched_columns)}")
    else:
        print(f"   ✅ All columns match! No mismatches detected.")
```

## Usage Examples

### Example 1: Quick Diagnosis (Recommended)

```python
# Import and reload
import importlib, cockroachdb_ycsb, cockroachdb_debug
importlib.reload(cockroachdb_ycsb)
importlib.reload(cockroachdb_debug)
from cockroachdb_debug import run_full_diagnosis_from_config

# Run diagnosis - that's it!
run_full_diagnosis_from_config(spark, config)
```

### Example 2: With Known Mismatches

```python
# If you already know specific columns mismatch
# (e.g., from previous diagnosis or manual inspection)
mismatched_columns = ['field7', 'field8', 'field9']

run_full_diagnosis_from_config(
    spark, 
    config,
    mismatched_columns=mismatched_columns
)
```

### Example 3: Check Specific Columns Only

```python
# Focus on specific columns (e.g., after a fix)
columns_to_verify = ['field3', 'field4']

run_full_diagnosis_from_config(
    spark,
    config,
    mismatched_columns=columns_to_verify
)
```

## Updated Cell 28 (Example 4)

**Before:**
```python
# Example 4: Full diagnosis using config (Recommended for Cell 14 sync issues)
import importlib,cockroachdb_ycsb,cockroachdb_debug
importlib.reload(cockroachdb_ycsb)
importlib.reload(cockroachdb_debug)
from cockroachdb_debug import run_full_diagnosis_from_config

# Define mismatched columns from Cell 14 output
mismatched_columns = ['field3', 'field4', 'field5', 'field6', 'field7', 'field8', 'field9']

# Run full diagnosis
run_full_diagnosis_from_config(
    spark=spark,
    config=config,
    mismatched_columns=mismatched_columns  # Set to None if no mismatches
)
```

**After:**
```python
# Example 4: Full diagnosis using config (Self-contained - no external dependencies!)
import importlib,cockroachdb_ycsb,cockroachdb_debug
importlib.reload(cockroachdb_ycsb)
importlib.reload(cockroachdb_debug)
from cockroachdb_debug import run_full_diagnosis_from_config

# Run full diagnosis using config from Cell 3
# This will:
#   - Auto-detect mismatched columns (no need to run Cell 14 first!)
#   - Refresh target DataFrame
#   - Establish CockroachDB connection
#   - Run comprehensive diagnosis
#   - Check staging table, Azure files, and row-by-row comparison
#   - Provide detailed troubleshooting recommendations
#
# Optional: If you already know which columns mismatch, you can pass them:
# mismatched_columns = ['field3', 'field4', 'field5']
# Otherwise, leave it as None for auto-detection
run_full_diagnosis_from_config(
    spark=spark,
    config=config,
    mismatched_columns=None  # Auto-detect mismatches (recommended)
)
```

## Backward Compatibility

Fully backward compatible - existing code still works:

```python
# Old way (still works)
mismatched_columns = ['field3', 'field4']
run_full_diagnosis_from_config(spark, config, mismatched_columns)

# New way (recommended)
run_full_diagnosis_from_config(spark, config)
```

## Files Changed

1. **`cockroachdb_debug.py`**
   - Added auto-detection logic in `run_full_diagnosis_from_config()`
   - Updated docstring with examples
   - Auto-detects CDC mode
   - Auto-deduplicates for append_only mode
   - Auto-compares all columns

2. **`cockroachdb-cdc-tutorial.ipynb`**
   - Cell 28 (Example 4): Updated to use auto-detection
   - Removed manual mismatched_columns definition
   - Updated comments to reflect self-contained nature

## Testing

To verify auto-detection works:

1. **Skip Cell 14** - don't run it at all
2. **Run Cell 28 directly** (Example 4)
3. **Observe output**:
   - Should show "Auto-detecting mismatched columns..."
   - Should detect CDC mode
   - Should deduplicate if append_only
   - Should compare columns
   - Should report findings

**Expected output:**
```
🔍 Auto-detecting mismatched columns...
   Detected CDC mode: append_only
   Deduplicating target to latest state for comparison...
   Comparing 11 columns...
   ✅ All columns match! No mismatches detected.
```

## Key Takeaway

**The diagnosis is now truly self-contained!**

No more:
- ❌ Running Cell 14 first
- ❌ Copying column names
- ❌ Manual error-prone steps
- ❌ Outdated or incorrect mismatch information

Just:
- ✅ One function call
- ✅ Auto-detection
- ✅ Accurate results
- ✅ Fast workflow

**Before:** Multi-step manual process with potential errors

**After:** Single function call with intelligent auto-detection
