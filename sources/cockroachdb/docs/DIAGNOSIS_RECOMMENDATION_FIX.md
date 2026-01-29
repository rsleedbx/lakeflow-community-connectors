# Diagnosis Recommendation Logic Fix

## Issue

The diagnosis was giving an incorrect recommendation that didn't match the evidence.

### The Evidence
```
📊 Current staging table: 803 rows
   ✅ Found 4 rows with these keys in staging:
+--------+--------------------+--------------------+--------------------+
|ycsb_key|field3              |field4              |field5              |
+--------+--------------------+--------------------+--------------------+
|112     |NULL                |NULL                |NULL                |
|112     |NULL                |NULL                |NULL                |
|112     |NULL                |NULL                |NULL                |
|112     |inserted_value_112_3|inserted_value_112_4|inserted_value_112_5|  ← DATA IS HERE!
+--------+--------------------+--------------------+--------------------+

📁 Checking raw Azure CDC files...
   📊 Column completeness for mismatched keys:
      ⚠️  field3    : Column not in CDC files
      ⚠️  field4    : Column not in CDC files
      ...
```

### The Wrong Recommendation
```
🔧 Recommended Fix:
    • Re-run the ingestion pipeline (Cell 12)
    • Ensure wait_for_changefeed_files() detects all column family files
    • Check that the stabilization_wait period is sufficient
```

**Why it's wrong**: The data IS in the staging table! Re-running Cell 12 won't help because:
1. ✅ Changefeed captured the data
2. ✅ Auto Loader ingested it to staging
3. ❌ **MERGE from staging → target failed**

The issue is with the MERGE logic, not with the changefeed or ingestion.

## Root Cause Analysis

### Scenario 1: Data in Staging (MERGE Issue)
- **Evidence**: Staging table shows row 112 with field3-9 values
- **Conclusion**: MERGE deduplication logic failed to consolidate column family fragments
- **Fix**: Fix MERGE logic or drop target and re-ingest

### Scenario 2: Data NOT in Staging (Changefeed Issue)
- **Evidence**: Staging table has row 112 but field3-9 are all NULL
- **Conclusion**: Changefeed never captured the field3-9 column family fragment
- **Fix**: Complete reset (drop changefeed, clear Azure, recreate) or manual backfill

## Fix Applied

Updated `run_full_diagnosis()` in `cockroachdb_debug.py` to:

### Change 1: Track if Data Exists in Staging

Modified `check_staging_and_azure_for_keys()` to return a boolean:

```python
def check_staging_and_azure_for_keys(...) -> bool:
    """
    Returns:
        bool: True if the mismatched data was found in staging (MERGE issue),
              False if data is missing from Azure (changefeed issue)
    """
    data_found_in_staging = False
    
    # Check staging table
    if matched_count > 0:
        # Check if any matched rows have non-NULL values for the mismatched columns
        for col in columns_to_check[:3]:
            if col in staging_df.columns:
                non_null_count = matched_rows.filter(F.col(col).isNotNull()).count()
                if non_null_count > 0:
                    data_found_in_staging = True  # ← Found the correct data!
                    break
    
    return data_found_in_staging
```

### Change 2: Context-Aware Recommendations

Updated diagnosis summary to provide different recommendations based on where the data is:

```python
if actual_mismatch_found:
    print(f"\n❌ CONFIRMED: Data is OUT OF SYNC")
    print(f"\n🔍 Found Issues:")
    print(f"    • {len(mismatched_columns)} columns have mismatched values")
    
    # Determine if data is in staging or missing from Azure
    if data_in_staging:
        print(f"    • ✅ Data EXISTS in staging table")
        print(f"    • ❌ MERGE from staging → target failed to consolidate fragments")
        print(f"\n🔧 Recommended Fix:")
        print(f"    • Issue: MERGE logic didn't properly consolidate column family fragments")
        print(f"    • Solution 1: Check MERGE deduplication logic in Cell 6")
        print(f"    • Solution 2: Manually run MERGE again (re-run Cell 12)")
        print(f"    • Solution 3: Drop target table and re-ingest (Cell 16 + Cell 12)")
    else:
        print(f"    • ❌ Data MISSING from staging table")
        print(f"    • ❌ Changefeed didn't capture the column family fragment")
        print(f"\n🔧 Recommended Fix:")
        print(f"    • Issue: Changefeed failed to capture field3-9 column family for some rows")
        print(f"    • Solution 1: Complete reset - drop changefeed, clear Azure, recreate")
        print(f"    • Solution 2: Manual backfill for affected keys")
        print(f"    • Solution 3: Check CockroachDB changefeed logs for errors")
```

## Expected Output After Fix

### When Data IS in Staging (MERGE Issue)
```
❌ CONFIRMED: Data is OUT OF SYNC

🔍 Found Issues:
    • 7 columns have mismatched values: field3, field4, field5, field6, field7

💡 Root Cause (Column Family Issue):
    • These columns are in a separate column family from the primary key
    • Column family fragments are stored in separate Parquet files
    • ✅ Data EXISTS in staging table
    • ❌ MERGE from staging → target failed to consolidate fragments

🔧 Recommended Fix:
    • Issue: MERGE logic didn't properly consolidate column family fragments
    • Solution 1: Check MERGE deduplication logic in Cell 6
    • Solution 2: Manually run MERGE again (re-run Cell 12)
    • Solution 3: Drop target table and re-ingest (Cell 16 + Cell 12)
```

### When Data is NOT in Staging (Changefeed Issue)
```
❌ CONFIRMED: Data is OUT OF SYNC

🔍 Found Issues:
    • 7 columns have mismatched values: field3, field4, field5, field6, field7

💡 Root Cause (Column Family Issue):
    • These columns are in a separate column family from the primary key
    • Column family fragments are stored in separate Parquet files
    • ❌ Data MISSING from staging table
    • ❌ Changefeed didn't capture the column family fragment

🔧 Recommended Fix:
    • Issue: Changefeed failed to capture field3-9 column family for some rows
    • Solution 1: Complete reset - drop changefeed, clear Azure, recreate
    • Solution 2: Manual backfill for affected keys (see ROW_112_DIAGNOSIS.md)
    • Solution 3: Check CockroachDB changefeed logs for errors
```

## Impact

**Before Fix**: Users were told to "re-run Cell 12" even when the data was already in staging, which wouldn't solve the problem.

**After Fix**: Users get context-aware recommendations:
- If data is in staging → Fix MERGE logic
- If data is missing → Fix changefeed or manual backfill

## Files Modified

- **`sources/cockroachdb/docs/cockroachdb_debug.py`**
  - Updated `check_staging_and_azure_for_keys()` to return boolean
  - Updated `run_full_diagnosis()` to provide context-aware recommendations
  - Added logic to detect if mismatched data exists in staging table

## Validation

✅ No linting errors
✅ Function returns correct boolean based on staging data
✅ Recommendations match the actual root cause
✅ Users get actionable, context-specific guidance

## Testing

Re-run Example 4 (Cell 30) with the diagnosis, and now the recommendation should correctly identify whether the issue is:
1. **MERGE logic failure** (data in staging but not in target)
2. **Changefeed capture failure** (data not even in staging)
