# Column Family Streaming Fix

## Problem

**Error**: `AnalysisException: [STREAMING_OUTPUT_MODE.UNSUPPORTED_OPERATION] Invalid streaming output mode: append`

When using `ingest_cdc_append_only_multi_family()` for column family CDC ingestion, the function failed because:
- Column family merging uses `groupBy` aggregations
- Spark Structured Streaming doesn't allow aggregations in **append mode** without a watermark
- The merge operation (`merge_column_family_fragments()`) cannot be applied directly on streaming DataFrames

## Root Cause

```python
# OLD (BROKEN):
merged_df = merge_column_family_fragments(transformed_df, ...)  # ← Aggregation on streaming DF
query = merged_df.writeStream.toTable(...)  # ← Fails: aggregation without watermark
```

The `merge_column_family_fragments()` function uses:
- `groupBy(primary_key_columns + ['_cdc_timestamp', '_cdc_operation'])`
- `.agg(first(col, ignorenulls=True))` for each data column

This is incompatible with streaming DataFrames in append mode.

## Solution: Two-Stage Approach

Use the same pattern as `ingest_cdc_with_merge_single_family`:

### Stage 1: Stream Raw Events (No Aggregations)
```python
# Stream raw CDC events to staging table (pure Spark, no aggregations)
query = (transformed_df.writeStream
    .format("delta")
    .option("checkpointLocation", f"{checkpoint_path}/data")
    .trigger(availableNow=True)
    .toTable(staging_table_fqn)  # ← No aggregations, Serverless compatible!
)
query.awaitTermination()
```

**Why it works**: No aggregations, no streaming limitations.

### Stage 2: Merge Column Families in Batch Mode
```python
# Read staging table as batch DataFrame
staging_df = spark.table(staging_table_fqn)

# Merge column family fragments (batch mode - no streaming limitations!)
merged_df = merge_column_family_fragments(staging_df, primary_key_columns, spark)

# Write to final target table (batch mode, append-only)
merged_df.write.format("delta").mode("append").saveAsTable(target_table_fqn)

# Clean up staging table
spark.sql(f"DROP TABLE IF EXISTS {staging_table_fqn}")
```

**Why it works**: Batch DataFrames have no restrictions on `groupBy` aggregations.

## Benefits

1. **Serverless Compatible**: No Python UDFs on workers (Stage 1 is pure Spark)
2. **No Streaming Limitations**: Aggregations run in batch mode (Stage 2)
3. **Consistent Pattern**: All 4 modes now use appropriate staging strategies
4. **Clean Separation**: Streaming ingestion vs. data transformation

## Implementation Details

### Staging Table Names
- `ingest_cdc_append_only_multi_family()`: Uses `{target_table}_staging_cf`
- `ingest_cdc_with_merge_multi_family()`: Uses `{target_table}_staging_cf`
- Both are automatically dropped after processing

### Processing Flow
```
Raw CDC Files (Azure)
    ↓ Stage 1 (Streaming)
Staging Table (fragments as-is)
    ↓ Stage 2 (Batch)
[Merge Fragments] → Coalesced Events
    ↓
[Deduplicate]     → (for MERGE mode only)
    ↓
Target Table
```

## Affected Functions

Both multi-family functions had the same issue:

### 1. `ingest_cdc_append_only_multi_family()` ✅ FIXED
- **Problem**: Called `merge_column_family_fragments()` on streaming DF before writing
- **Fix**: Two-stage approach
  - Stage 1: Stream raw events to staging table
  - Stage 2: Batch merge fragments → write to target

### 2. `ingest_cdc_with_merge_multi_family()` ✅ FIXED  
- **Problem**: Called `merge_column_family_fragments()` on streaming DF before writing to staging
- **Fix**: Two-stage approach with batch merging
  - Stage 1: Stream raw events to staging table
  - Stage 2: Batch merge fragments → deduplicate → MERGE to target

## Related Functions (Not Affected)

- `ingest_cdc_with_merge_single_family()`: Already uses two-stage (no column families)
- `ingest_cdc_append_only_single_family()`: NOT affected (no aggregations at all)

## Testing

```python
# In Cell 2: Set to multi_cf mode
cdc_mode = "append-only"
column_family_mode = "multi_cf"

# Run cells in order
# Cell 7: Create table with multiple column families
# Cell 8: Insert snapshot data
# Cell 9: Create changefeed with split_column_families
# Cell 10: Run CDC workload
# Cell 12: Read CDC events (will use two-stage approach)
# Cell 13: Query results (should show merged fragments)
```

## Date: 2026-01-28

Fixed by implementing two-stage approach for column family merging in append-only mode.
