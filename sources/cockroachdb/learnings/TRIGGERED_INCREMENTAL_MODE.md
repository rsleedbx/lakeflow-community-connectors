# Triggered Incremental Mode (30-Minute Batch Processing)

## Overview

The CockroachDB connector now supports **triggered incremental mode** for scheduled pipelines that run at regular intervals (e.g., every 30 minutes). This mode:

1. ✅ Processes only available changes (doesn't wait indefinitely)
2. ✅ Stops immediately when caught up (no waiting on empty tables)
3. ✅ Tracks cursor automatically between runs
4. ✅ Handles both initial snapshot and incremental updates

## How It Works

### First Run (No Cursor)
```
Pipeline starts → Loads full snapshot → Saves cursor → Stops
```

### Subsequent Runs (Has Cursor)
```
Pipeline starts → Resumes from cursor → Processes all queued changes 
  → Receives "resolved" timestamp (caught up signal) 
  → Stops immediately → Saves new cursor
```

### Key Mechanism: Resolved Timestamps

CockroachDB changefeeds emit **resolved timestamps** to signal:
> "All changes up to this timestamp have been sent. You're caught up!"

The connector detects this signal and **stops immediately** instead of:
- ❌ Waiting for `batch_size` to be reached
- ❌ Timing out after `query_timeout`
- ❌ Streaming indefinitely

## Configuration

### Recommended Settings

```python
default_table_config = {
    "initial_scan": "yes",              # Enables cursor-based incremental mode
    "resolved_interval": "1s",          # How often to check if caught up (fast!)
    "target_rows": "12000",             # Safety limit per run
    "split_column_families": "true",    # Required for multi-column tables
    "coalesce_split_families": "true",  # Merge fragmented events
}
```

### Key Parameters

| Parameter | Value | Purpose |
|-----------|-------|---------|
| `initial_scan` | `"yes"` | Enables cursor tracking (required for incremental mode) |
| `resolved_interval` | `"1s"` | Fast detection when caught up (1 second polling) |
| `target_rows` | `12000` | Safety limit to prevent runaway queries |

**Note**: `resolved_interval` is critical for fast stopping. With `"1s"`, the connector checks every second if it's caught up. This means:
- **No changes**: Stops within ~1 second
- **Small changes**: Processes them, then stops within ~1 second
- **Large changes**: Processes up to `target_rows`, then stops on next run

## Example Run Sequence

### Scenario: Table with 10,000 initial rows, then 50 updates every 30 minutes

#### Run 1 (T=0:00)
```
⏱️  Step 1: Validating table exists...
✅ Table validated

🆕 Initial run: No cursor found, starting from beginning

⏱️  Step 6: Collecting events from generator...
   📊 Progress: 110000 events processed (164520.0 events/sec)

⏱️  Step 7: Coalescing 110000 fragmented events...
✅ Coalesced to 10000 complete rows

Total events returned: 10000
End offset: {"cursor": "1734562262.0000000000"}  ← Saved for next run
```

#### Run 2 (T=0:30)
```
🔄 Incremental batch mode: Resuming from cursor (has previous state)
Cursor: 1734562262.0000000000

⏱️  Step 6: Collecting events from generator...
   📊 Progress: 550 events processed (50 rows with 11 column families)
   ✅ Received resolved timestamp (caught up): 1734564062.0000000000
   📊 Processed 550 events before catching up
   ⏹️  Stopping (no more queued changes)

⏱️  Step 7: Coalescing 550 fragmented events...
✅ Coalesced to 50 complete rows

Total events returned: 50
End offset: {"cursor": "1734564062.0000000000"}
Total time: 2.1s  ← Fast! Stopped immediately when caught up
```

#### Run 3 (T=1:00) - No Changes
```
🔄 Incremental batch mode: Resuming from cursor
Cursor: 1734564062.0000000000

⏱️  Step 6: Collecting events from generator...
   📊 Progress: 0 events processed
   ✅ Received resolved timestamp (caught up): 1734565862.0000000000
   📊 Processed 0 events before catching up
   ⏹️  Stopping (no more queued changes)

Total events returned: 0
End offset: {"cursor": "1734565862.0000000000"}
Total time: 1.2s  ← Very fast! Nothing to process
```

## Benefits

### 1. **No Wasted Time**
- Tables with no changes complete in ~1 second
- No waiting for timeout or batch_size

### 2. **Efficient Resource Usage**
- Connector stops immediately when caught up
- Databricks cluster idles between triggers

### 3. **Predictable Behavior**
- First run: Full snapshot
- Subsequent runs: Incremental catch-up
- Always stops after processing available data

### 4. **Safety Limits**
- `target_rows` prevents runaway queries
- `query_timeout` prevents indefinite hangs
- If limits hit, next run continues from cursor

## Comparison with Other Modes

| Mode | `initial_scan` | Stops When | Use Case |
|------|---------------|------------|----------|
| **Snapshot Only** | `"only"` | Snapshot complete | One-time migration, testing |
| **Triggered Incremental** | `"yes"` | Caught up (resolved) | Scheduled pipelines (30-min trigger) |
| **Continuous Streaming** | `"continuous"` | Never (indefinite) | Real-time CDC (not yet implemented) |

## Technical Details

### Resolved Timestamp Detection

The connector detects resolved timestamps in the changefeed output:

```python
# Check if this is a resolved timestamp event
if key_json is None and value_json is None:
    # Resolved timestamp - marks "all changes up to this time have been sent"
    last_resolved = updated
    
    # For incremental mode (cursor-based), stop after first resolved
    if cursor is not None:
        print(f"✅ Received resolved timestamp (caught up): {last_resolved}")
        print(f"⏹️  Stopping (no more queued changes)")
        break  # Exit immediately
```

### Cursor Persistence

DLT automatically persists cursors between runs:
1. Connector returns `end_offset = {"cursor": "timestamp"}`
2. DLT checkpoints this in the Delta table metadata
3. Next run: DLT passes `start_offset = {"cursor": "timestamp"}` to connector
4. Connector resumes from that timestamp

### Ingestion Type

For triggered mode, we use `ingestion_type='snapshot'` (not `'cdc'`):
- Each run is treated as a **batch/snapshot**
- DLT uses `apply_changes_from_snapshot()` for each batch
- Cursor tracking happens behind the scenes
- Pipeline completes after each run (doesn't stream indefinitely)

## Troubleshooting

### Pipeline Runs Too Long
**Symptom**: Pipeline takes 10+ minutes to complete when there are no changes

**Cause**: `resolved_interval` is too large (e.g., `"10s"` or `"30s"`)

**Fix**: Set `resolved_interval: "1s"` for faster detection

### Pipeline Hits Batch Size Limit Frequently
**Symptom**: Logs show "Reached batch size limit" warnings

**Cause**: `target_rows` is too small for the change volume

**Fix**: Increase `target_rows` to handle larger batches:
```python
"target_rows": "50000",  # 50K rows per run
```

### First Run Takes Very Long
**Symptom**: First run (snapshot) takes hours

**Cause**: Large table with millions of rows

**Fix**: This is expected for initial snapshot. Subsequent incremental runs will be fast.

## Next Steps

1. **Deploy the updated connector**:
   ```bash
   cd /path/to/lakeflow-community-connectors/sources/cockroachdb
   ./scripts/copydir.sh
   ```

2. **Start a full refresh** to reset state:
   ```bash
   databricks pipelines start-update 7d02d189-2d1b-4331-aee7-acf24bcd9225 --full-refresh
   ```

3. **Configure pipeline trigger**:
   - Set to run every 30 minutes
   - Monitor first few runs to verify behavior

4. **Monitor performance**:
   - Check run durations (should be 1-5 min for incremental)
   - Verify cursor advancement
   - Confirm data freshness

## Summary

**Triggered incremental mode** provides the best of both worlds:
- ✅ Efficient (stops when caught up)
- ✅ Complete (processes all available changes)
- ✅ Automatic (cursor tracking handled by DLT)
- ✅ Flexible (works with any trigger interval)

Perfect for **scheduled batch CDC pipelines** that run every 15-60 minutes!








