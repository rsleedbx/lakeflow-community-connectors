# Code Deduplication Refactoring Plan

## Problem

We have **duplicate CDC operation detection logic** in production and test paths:

### Current Duplication:

```python
# PRODUCTION PATH: _add_cdc_metadata_to_dataframe() (Line 1093-1123)
if snapshot_cutoff:
    df = df.withColumn("_cdc_operation",
        F.when(
            (F.col("__crdb__event_type") == "c") & 
            (F.col("__crdb__updated") <= F.lit(snapshot_cutoff)),
            F.lit("SNAPSHOT")
        )
        .when(
            (F.col("__crdb__event_type") == "c") & 
            (F.col("__crdb__updated") > F.lit(snapshot_cutoff)),
            F.lit("UPDATE")
        )
        .when(F.col("__crdb__event_type") == "i", F.lit("INSERT"))
        .when(F.col("__crdb__event_type") == "d", F.lit("DELETE"))
        ...
    )

# TEST PATH: analyze_azure_changefeed_files() (Line 3271-3289)
if after and not before:
    if snapshot_cutoff and event_timestamp:
        if event_timestamp <= snapshot_cutoff:
            cdc_operation = 'SNAPSHOT'
        else:
            cdc_operation = 'INSERT'
    else:
        cdc_operation = 'SNAPSHOT'
    row_data = after
elif after and before:
    cdc_operation = 'UPDATE'
    row_data = after
elif before and not after:
    cdc_operation = 'DELETE'
    row_data = before
```

**Problem**: These implement the same business logic but aren't sharing code!

## Refactoring Plan

###  Phase 1: Create Shared JSON Operation Detection Method ✅ PRIORITY

```python
def _determine_cdc_operation_json(
    self,
    before: Dict = None,
    after: Dict = None, 
    event_timestamp: str = None,
    snapshot_cutoff: str = None
) -> tuple[str, Dict]:
    """
    Determine CDC operation for JSON wrapped envelope format.
    
    Args:
        before: 'before' field from JSON event
        after: 'after' field from JSON event
        event_timestamp: 'updated' timestamp from JSON event
        snapshot_cutoff: Snapshot cutoff timestamp
        
    Returns:
        Tuple of (cdc_operation, row_data)
        - cdc_operation: 'SNAPSHOT', 'INSERT', 'UPDATE', 'DELETE', or 'UNKNOWN'
        - row_data: The data dictionary to use (before or after)
    """
    if after and not before:
        # Use timestamp to distinguish SNAPSHOT from INSERT
        if snapshot_cutoff and event_timestamp:
            if event_timestamp <= snapshot_cutoff:
                return ('SNAPSHOT', after)
            else:
                return ('INSERT', after)
        else:
            # No timestamp info - assume SNAPSHOT (safe default)
            return ('SNAPSHOT', after)
    elif after and before:
        return ('UPDATE', after)
    elif before and not after:
        return ('DELETE', before)
    else:
        return ('UNKNOWN', None)
```

### Phase 2: Extend Existing Parquet Method

Update `_determine_cdc_operation()` to handle both formats:

```python
def _determine_cdc_operation(
    self, 
    event_type: str = None,
    event_timestamp: str = None,
    snapshot_cutoff: str = None,
    before: Dict = None,
    after: Dict = None
) -> str:
    """
    Map CockroachDB event to CDC operation.
    
    Supports both:
    - Parquet format: Uses event_type ('c', 'd') + timestamp
    - JSON format: Uses before/after fields + timestamp
    
    Args:
        event_type: CockroachDB event type ('c', 'i', 'd') for Parquet
        event_timestamp: Timestamp from __crdb__updated or 'updated'
        snapshot_cutoff: Cutoff timestamp to distinguish snapshot from CDC
        before: 'before' field for JSON format
        after: 'after' field for JSON format
        
    Returns:
        CDC operation: 'SNAPSHOT', 'INSERT', 'UPDATE', 'DELETE', 'UNKNOWN'
    """
    # JSON format detection
    if before is not None or after is not None:
        if after and not before:
            if snapshot_cutoff and event_timestamp:
                return 'SNAPSHOT' if event_timestamp <= snapshot_cutoff else 'INSERT'
            return 'SNAPSHOT'
        elif after and before:
            return 'UPDATE'
        elif before and not after:
            return 'DELETE'
        return 'UNKNOWN'
    
    # Parquet format detection (existing logic)
    if event_type == 'c':
        if event_timestamp and snapshot_cutoff:
            try:
                if event_timestamp > snapshot_cutoff:
                    return 'UPDATE'
            except:
                pass
        return 'SNAPSHOT'
    elif event_type == 'i':
        return 'INSERT'
    elif event_type == 'd':
        return 'DELETE'
    
    return 'UNKNOWN'
```

### Phase 3: Refactor `analyze_azure_changefeed_files()` to Use Shared Method

**BEFORE** (Duplicate Logic):
```python
if after and not before:
    if snapshot_cutoff and event_timestamp:
        if event_timestamp <= snapshot_cutoff:
            cdc_operation = 'SNAPSHOT'
        else:
            cdc_operation = 'INSERT'
    else:
        cdc_operation = 'SNAPSHOT'
    row_data = after
elif after and before:
    cdc_operation = 'UPDATE'
    row_data = after
elif before and not after:
    cdc_operation = 'DELETE'
    row_data = before
else:
    continue
```

**AFTER** (Calling Shared Method):
```python
# Use shared CDC operation detection
connector_temp = LakeflowConnect({})
cdc_operation = connector_temp._determine_cdc_operation(
    event_timestamp=event_timestamp,
    snapshot_cutoff=snapshot_cutoff,
    before=before,
    after=after
)

if cdc_operation == 'UNKNOWN':
    continue

# Get row data based on operation
if cdc_operation == 'DELETE':
    row_data = before
else:
    row_data = after
```

### Phase 4: Create Shared Snapshot Cutoff Detection

```python
def _detect_snapshot_cutoff_from_files(
    self,
    data_blobs: List[str],
    format_type: str,
    blob_service_client,
    container_name: str,
    debug: bool = False
) -> Optional[str]:
    """
    Detect snapshot cutoff timestamp from sequence 00000000 files.
    
    Works for both JSON and Parquet formats.
    
    Args:
        data_blobs: List of blob names
        format_type: 'json' or 'parquet'
        blob_service_client: Azure blob service client
        container_name: Container name
        debug: Enable debug output
        
    Returns:
        Max timestamp from snapshot files, or None if not found
    """
    max_timestamp = None
    
    for blob_name in data_blobs:
        # Check if it's a snapshot file (sequence 00000000)
        if '-00000000-' not in blob_name:
            continue
            
        try:
            blob_client = blob_service_client.get_blob_client(
                container=container_name,
                blob=blob_name
            )
            download_stream = blob_client.download_blob()
            
            if format_type == 'json':
                # Parse JSON lines
                content = download_stream.readall().decode('utf-8')
                for line in content.strip().split('\n'):
                    if not line:
                        continue
                    try:
                        event_data = json.loads(line)
                        timestamp_str = event_data.get('updated', '')
                        if timestamp_str:
                            if max_timestamp is None or timestamp_str > max_timestamp:
                                max_timestamp = timestamp_str
                    except:
                        continue
                        
            else:  # parquet
                # Parse Parquet
                import pandas as pd
                from io import BytesIO
                df = pd.read_parquet(BytesIO(download_stream.readall()))
                if '__crdb__updated' in df.columns:
                    file_max = df['__crdb__updated'].max()
                    if max_timestamp is None or (file_max and file_max > max_timestamp):
                        max_timestamp = file_max
                        
        except Exception as e:
            if debug:
                print(f"   Warning: Could not read timestamp from {blob_name}: {e}")
            continue
    
    return max_timestamp
```

## Benefits of Refactoring

1. ✅ **Single Source of Truth**: One place to fix CDC detection bugs
2. ✅ **Test Validates Production**: Test path uses exact same logic as production
3. ✅ **Maintainability**: Changes to CDC rules update everywhere automatically
4. ✅ **Consistency**: Parquet and JSON use unified detection logic
5. ✅ **Reduced Code**: ~100 lines of duplication eliminated

## Risks & Mitigation

| Risk | Mitigation |
|------|------------|
| Breaking production | Keep existing code, add new shared methods first |
| Test failures during migration | Migrate one function at a time, verify each |
| Regression in analysis | Run full test matrix after each change |

## Implementation Order

1. ✅ **Create `_determine_cdc_operation_json()` method** (new, doesn't break anything)
2. ✅ **Create `_detect_snapshot_cutoff_from_files()` method** (new, doesn't break anything)
3. ✅ **Refactor `analyze_azure_changefeed_files()` to use shared methods**
4. ✅ **Refactor `analyze_volume_changefeed_files()` to use shared methods**
5. ✅ **Run full test matrix to verify**
6. ✅ **Remove old duplicate code**
7. ✅ **Update documentation**

## Timeline

- Phase 1-2: Create shared methods (30 minutes)
- Phase 3-4: Refactor analysis functions (1 hour)
- Testing & Verification: (1 hour)
- **Total: 2-3 hours**

## Success Criteria

1. All tests pass with identical results
2. Zero duplication of CDC detection logic
3. Production and test paths call same core methods
4. Code is easier to understand and maintain

## Related Documentation

- `JSON_INSERT_DETECTION_FIX.md` - The duplicate code this refactoring will eliminate
- `CODE_CLEANUP_ANALYSIS.md` - Earlier deduplication efforts
- `REFACTORING_SUMMARY.md` - Overall refactoring goals


