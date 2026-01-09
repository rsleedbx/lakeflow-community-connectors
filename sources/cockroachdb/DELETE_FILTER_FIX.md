# DELETE Operation Filter Fix

## Issue

Delta table was including deleted rows, causing a mismatch with the expected row count.

### Symptoms

```
📊 Comparison:
   Delta: 1,050
   Source: 950
   
   ⚠️  MISMATCH: +100 rows
```

**Expected behavior:**
- Start: 1,000 rows (snapshot)
- Insert: +50 rows → 1,050 rows
- Update: 400 rows → 1,050 rows  
- Delete: -100 rows → **950 rows final**

**Actual behavior:**
- Delta table had 1,050 rows (950 active + 100 deleted)

## Root Cause

The `load_and_merge_cdc_to_delta` function uses Structured Streaming with `outputMode("complete")` to write to Delta:

```python
df_merged.writeStream
    .format("delta")
    .outputMode("complete")  # ← Writes ALL rows in result
    .toTable(target_table_path)
```

**What happens:**
1. Column family fragments are merged by primary key
2. After merge, each unique key has ONE row with the latest `_cdc_operation`
3. For deleted keys, this row has `_cdc_operation = 'DELETE'`
4. `outputMode("complete")` writes **all rows** to Delta, including DELETEs
5. Result: Deleted rows remain in the Delta table

**Why it's wrong:**
- `outputMode("complete")` replaces the entire table with the current aggregated state
- It doesn't perform MERGE operations with DELETE clauses
- DELETE rows are treated as regular data rows and written to the table

## The Fix

Added a filter step before writing to Delta (line 5139-5149):

```python
# ========================================================================
# Step 5.5: Filter out DELETE operations
# ========================================================================
# Complete output mode writes ALL rows, so we need to filter out DELETEs
# before writing to ensure deleted rows don't appear in the final table
if debug:
    print("🗑️  Filtering deleted rows...")

df_active = df_merged.filter(F.col("_cdc_operation") != "DELETE")

if debug:
    print("   ✅ DELETE operations filtered out")
    print("   (Only SNAPSHOT, INSERT, UPDATE rows will be written)")
    print()

# Write df_active instead of df_merged
query = (df_active.writeStream
    .format("delta")
    .outputMode("complete")
    ...
)
```

## How It Works

### Before Fix

```
Source Events (1,550 raw → 1,050 coalesced):
├── SNAPSHOT: 0
├── INSERT: 0
├── UPDATE: 950
└── DELETE: 100

After Merge (1,050 rows):
├── 950 rows with _cdc_operation = UPDATE
└── 100 rows with _cdc_operation = DELETE

Delta Table (1,050 rows):  ← WRONG!
├── 950 active rows
└── 100 deleted rows  ← Should not be here!
```

### After Fix

```
Source Events (1,550 raw → 1,050 coalesced):
├── SNAPSHOT: 0
├── INSERT: 0
├── UPDATE: 950
└── DELETE: 100

After Merge (1,050 rows):
├── 950 rows with _cdc_operation = UPDATE
└── 100 rows with _cdc_operation = DELETE

After Filter (950 rows):  ← NEW STEP!
└── 950 rows with _cdc_operation != DELETE

Delta Table (950 rows):  ← CORRECT!
└── 950 active rows only
```

## Testing

Run the notebook with `version=0` to use the latest timestamped test data:

```python
TEST_VERSION = 0  # Use latest test run

result = load_and_merge_cdc_to_delta(
    source_table=SOURCE_TABLE,
    volume_path=VOLUME_PATH,
    target_table_path=TARGET_TABLE_PATH,
    crdb_config=crdb_config,
    catalog=CRDB_CATALOG,
    schema=CRDB_SCHEMA,
    spark=spark, 
    dbutils=dbutils,
    clear_checkpoint=True,
    verify=True,
    compare_source=True,
    debug=True,
    version=TEST_VERSION
)
```

**Expected output:**

```
🗑️  Filtering deleted rows...
   ✅ DELETE operations filtered out
   (Only SNAPSHOT, INSERT, UPDATE rows will be written)

📊 Comparison:
   Delta: 950
   Source: 950
   
   ✅✅✅ PERFECT MATCH! ✅✅✅
```

## Why Not Use MERGE Instead?

**Q:** Why not use Delta MERGE with DELETE clause instead of filtering?

**A:** 
1. **Streaming limitation:** Spark Structured Streaming doesn't support `foreachBatch` with conditional MERGE/DELETE in `outputMode("complete")`
2. **Simplicity:** Filtering is cleaner and more efficient for streaming aggregations
3. **Performance:** Complete mode rewrites the entire table anyway, so filtering before write is optimal

## Implementation Details

**File modified:** `sources/cockroachdb/cockroachdb.py`

**Function:** `load_and_merge_cdc_to_delta()`

**Changes:**
- Added Step 5.5: Filter DELETE operations
- Changed `df_merged` to `df_active` in writeStream call
- Added debug messages for transparency

**Line numbers:** ~5139-5164

## Related Issues

This fix addresses the same underlying issue that was causing operation count mismatches in the test matrix:
- Column family merge was working correctly
- DELETE detection was working correctly
- The only issue was that DELETEs weren't being excluded from the final table

## Backward Compatibility

✅ **Fully backward compatible:**
- No API changes
- No parameter changes
- Only affects internal processing logic
- Existing notebooks and scripts work unchanged

