# CockroachDB Connector - Volume Mode

## Overview

The `cockroachdb.py` connector now supports **three modes**:

1. **Direct Mode** - Sinkless changefeed (testing/development)
2. **Azure Parquet Mode** - Read from Azure Blob Storage
3. **Volume Mode** - Read from Unity Catalog Volume ✨ NEW

## Volume Mode Implementation

### Configuration

```python
df = spark.readStream.format("lakeflow_connect") \
    .option("connector_module", "cockroachdb") \
    .option("connector_class", "LakeflowConnect") \
    .option("volume_path", "/Volumes/main/schema/volume_name") \  # Triggers Volume mode
    .option("schema", "public") \
    .load("table_name")
```

### How It Works

#### 1. **File Discovery**
- Lists Parquet files in Volume using `dbutils.fs.ls()` or Spark
- Filenames contain embedded timestamps (e.g., `202512191714242809831900000000000-...`)
- Sorts files by name (chronological order)

#### 2. **Cursor-Based Processing**
```python
cursor = {
    "cursor": "202512191714242809831900000000000-d340a6adc87c635b-1-375-00000000-..."
}
```
- Tracks last processed filename
- On restart, processes only files > cursor
- **Exactly-once processing** guaranteed

#### 3. **Snapshot vs CDC**
- Both handled uniformly (no explicit distinction)
- All events have `updated` timestamp
- Events ordered by file timestamp, then `updated`

#### 4. **CDC Format Transformation**
```python
# Input (Parquet row):
{
    "key": ["user123"],
    "after": {"field0": "value0", "field1": "value1", ...},
    "updated": "1734623682809831900.0000000000"
}

# Output (transformed):
{
    "field0": "value0",        # Extracted from 'after'
    "field1": "value1",
    "_cdc_key": ["user123"],
    "_cdc_updated": "1734623682809831900.0000000000",
    "_cdc_operation": "UPSERT",
    "_source_file": "202512191714242809831900000000000-..."
}
```

#### 5. **DELETE Handling**
```python
# When after is NULL:
{
    "key": ["user123"],
    "after": null,
    "updated": "1734623682809831900.0000000000"
}
# Transforms to:
{
    "_cdc_key": ["user123"],
    "_cdc_updated": "1734623682809831900.0000000000",
    "_cdc_operation": "DELETE",
    "_source_file": "..."
}
```

## Architecture

```
CockroachDB → Azure Blob Storage
                    ↓
            (sync_azure_to_volume.sh)
                    ↓
          Unity Catalog Volume
                    ↓
          cockroachdb.py (Volume Mode)
                    ↓
              Delta Tables
```

## Benefits

✅ **No Database Connection** - Reads pre-synced files  
✅ **No External Credentials** - Volume uses UC permissions  
✅ **Unity Catalog Governance** - Full UC audit trail  
✅ **Exactly-Once Processing** - File-based cursor tracking  
✅ **Resumable** - Picks up where it left off  
✅ **No Vendor Lock-in** - Works with any Parquet CDC files  

## Testing

### 1. Sync Files to Volume
```bash
cd sources/cockroachdb_s3/scripts
./sync_azure_to_volume.sh
```

### 2. Run Volume Mode Test
```bash
cd sources/cockroachdb/scripts
./test_volume_mode.sh
```

### 3. Or Test Manually in Notebook
```python
# In Databricks notebook
df = spark.readStream.format("lakeflow_connect") \
    .option("connector_module", "cockroachdb") \
    .option("connector_class", "LakeflowConnect") \
    .option("volume_path", "/Volumes/main/robert_lee_cockroachdb/parquet_files") \
    .option("schema", "public") \
    .load("usertable")

df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/tmp/test/_checkpoint") \
    .trigger(availableNow=True) \
    .start("/tmp/test/output")
```

## Comparison: All Three Modes

| Feature | Direct | Azure Parquet | Volume |
|---------|--------|---------------|--------|
| **Connection** | Database | Database + Azure | None |
| **Data Source** | Live query | Azure Blob | UC Volume |
| **Credentials** | DB password | DB + Azure key | UC only |
| **Real-time** | Yes | Via changefeed | No (batch) |
| **Resumable** | Timestamp | Filename | Filename |
| **Use Case** | Dev/Test | Production CDC | Pre-synced data |
| **UC Governance** | ❌ | ❌ | ✅ |

## Implementation Details

### Code Changes in `cockroachdb.py`

1. **__init__ method (lines 114-149)**
   - Added `volume_path` detection
   - Priority: volume > azure > direct

2. **read_table method (lines 347-370)**
   - Routes to `_read_table_from_volume()` if mode == "volume"

3. **_read_table_from_volume method (lines 711-854)**
   - New method implementing Volume reading logic
   - File listing, cursor filtering, Parquet reading, CDC transformation

### Cursor State Example

```python
# Initial run
start_offset = {}  # No cursor

# After processing 3 files
end_offset = {
    "cursor": "202512191714242809831900000000000-d340a6adc87c635b-1-375-00000002-..."
}

# Next run (resumption)
start_offset = end_offset  # Picks up where left off
```

## Troubleshooting

### No Files Found
- Verify Volume path: `dbutils.fs.ls("/Volumes/main/schema/volume_name")`
- Check files are `.parquet` extension
- Run `sync_azure_to_volume.sh` to populate Volume

### Permission Errors
- Ensure UC Volume is accessible
- Check schema permissions: `GRANT USE SCHEMA ON schema TO user`
- Check volume permissions: `GRANT READ VOLUME ON volume TO user`

### Cursor Not Advancing
- Check that files have different names (timestamp changes)
- Verify cursor format matches filename prefix
- Look for errors in Parquet reading (check logs)

## Future Enhancements

- [ ] Support split column family coalescing for Volume files
- [ ] Add file-level parallelism (process multiple files concurrently)
- [ ] Support schema evolution tracking
- [ ] Add metrics (files processed, rows read, cursor position)
- [ ] Support Volume-native CDC format (if CockroachDB adds)

