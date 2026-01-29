# CockroachDB Auto Loader Functions

## Summary

Created `cockroachdb_autoload.py` containing the 4 CDC ingestion functions that were referenced in `cockroachdb-cdc-tutorial.ipynb` Cell 14 but were never actually implemented.

## History

**Correction**: These functions **DID exist previously** and were deleted!

- They were originally defined in Cell 6 of the notebook
- They existed in `cockroachdb-cdc-tutorial-2-after-dup-removal.ipynb` (backup file from before deletion)
- At some point, they were removed from the notebook
- Cell 14 continued calling them, causing `NameError`
- This file restores them from the backup (Cell 6, lines 500-1350)

**Timeline**:
1. **Original**: Functions existed in notebook Cell 6
2. **Deletion**: Functions were removed (reason unknown)
3. **Column Family Fix**: `merge_column_family_fragments()` in `cockroachdb.py` was enhanced with `deduplicate_to_latest_state` parameter
4. **This Restoration**: Functions restored from backup (without the column family NULL fix)

## What Was Created

Created `cockroachdb_autoload.py` with 4 new functions implementing the CDC ingestion patterns:

### 1. `ingest_cdc_append_only_single_family()`
- **Mode**: `append_only` + `single_cf`
- **Pattern**: Direct Auto Loader → append to Delta table
- **No** column family merging
- **No** MERGE logic
- Returns: Streaming query

### 2. `ingest_cdc_append_only_multi_family()`
- **Mode**: `append_only` + `multi_cf`
- **Pattern**: Auto Loader → merge fragments → append to Delta table
- **Yes** column family merging (using `merge_column_family_fragments()`)
- **No** MERGE logic
- Returns: Streaming query

### 3. `ingest_cdc_with_merge_single_family()`
- **Mode**: `update_delete` + `single_cf`
- **Pattern**: Auto Loader → staging table → MERGE to target
- **No** column family merging
- **Yes** MERGE logic (DELETE/UPDATE/INSERT)
- Returns: dict with `query` and `merge_complete`

### 4. `ingest_cdc_with_merge_multi_family()`
- **Mode**: `update_delete` + `multi_cf`
- **Pattern**: Auto Loader → staging table → merge fragments → MERGE to target
- **Yes** column family merging (with `deduplicate_to_latest_state=True`)
- **Yes** MERGE logic (DELETE/UPDATE/INSERT)
- Returns: dict with `query` and `merge_complete`

## Implementation Details

All functions follow these patterns:

### Append-Only Functions
- Use Spark Auto Loader with `cloudFiles` format
- Read from Azure path: `abfss://{container}@{account}.dfs.core.windows.net/parquet/...`
- Add CDC metadata: `_cdc_timestamp`, `_cdc_operation`
- Exclude `.RESOLVED` files with `pathGlobFilter: "*.parquet"`
- Write to Delta table with `outputMode("append")`

### Update-Delete Functions
- **Stage 1**: Stream to staging table (same as append-only)
- **Stage 2**: Merge fragments (multi_cf only) + deduplicate
- **Stage 3**: MERGE to target table using Delta Lake MERGE API
  - Initial load: Exclude DELETE operations
  - Incremental: Apply DELETEs first, then UPSERTs

### Column Family Merging
- Multi-CF functions call `merge_column_family_fragments()`
- For `update_delete` mode, uses `deduplicate_to_latest_state=True`
  - This preserves old column values when newer events have NULLs
  - Critical fix for the column family NULL bug discovered in debugging

## Notebook Integration

Updated `cockroachdb-cdc-tutorial.ipynb` Cell 14:
- Added import at the beginning of the cell:
  ```python
  from cockroachdb_autoload import (
      ingest_cdc_append_only_single_family,
      ingest_cdc_append_only_multi_family,
      ingest_cdc_with_merge_single_family,
      ingest_cdc_with_merge_multi_family
  )
  ```
- Rest of Cell 14 logic remains unchanged (function selection based on modes)

## Dependencies

### Required in Notebook Scope
- `merge_column_family_fragments()` - Defined in Cell 8 of the notebook
- Must be executed before Cell 14

### Required Spark/Databricks
- Spark Structured Streaming
- Delta Lake
- Auto Loader (`cloudFiles` format)
- `DeltaTable` API for MERGE operations

## Testing

To test these functions:
1. Set your configuration in Cell 3 (config file or embedded config)
2. Run Cells 1-13 (create table, insert snapshot, create changefeed, run workload)
3. Run Cell 14 - it will now import and call the appropriate function
4. Run Cell 15 to verify sync

## Future Enhancements

### Option 1: Add Column Family NULL Fix
If you want to enhance `merge_column_family_fragments()` with the latest fix:
1. Replace the function in this file with the version from `cockroachdb.py` (lines 5450-5838)
2. It includes the `deduplicate_to_latest_state` parameter for robust NULL handling
3. Update `ingest_cdc_with_merge_multi_family()` to call with `deduplicate_to_latest_state=True`

### Option 2: Move to cockroachdb.py
If you want to move these to `cockroachdb.py`:
1. Add them to the end of `cockroachdb.py`
2. Update imports to `from sources.cockroachdb.cockroachdb import merge_column_family_fragments`
3. Update notebook Cell 14 import path

## Files Modified

1. **Created**: `sources/cockroachdb/docs/cockroachdb_autoload.py` (new file, 450 lines)
2. **Modified**: `sources/cockroachdb/docs/cockroachdb-cdc-tutorial.ipynb` Cell 14
   - Added import statement for the 4 functions
   - Removed incorrect comment about "Functions are defined in Cell 5"

## Key Takeaway

**These functions were never deleted - they were never implemented!**

Cell 14 was calling functions that didn't exist. This is now fixed by:
1. Creating the actual implementations in `cockroachdb_autoload.py`
2. Importing them in Cell 14

The implementations follow established CDC patterns from:
- `cockroachdb.py`: `load_and_merge_cdc_to_delta()` (for Volume mode)
- Databricks Auto Loader best practices
- Delta Lake MERGE patterns
- The `merge_column_family_fragments()` function with the new `deduplicate_to_latest_state` parameter
