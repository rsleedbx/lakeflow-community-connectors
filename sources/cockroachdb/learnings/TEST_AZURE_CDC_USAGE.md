# test_azure_cdc.sh - Usage Guide

## Overview

End-to-end test script for CockroachDB → Azure Blob Storage CDC with **configurable format** support.

## Syntax

```bash
./test_azure_cdc.sh [FORMAT]
```

**FORMAT**: `parquet` (default) or `json`

---

## Examples

### Test with Parquet Format (Default)

```bash
cd sources/cockroachdb/scripts
./test_azure_cdc.sh
```

or explicitly:

```bash
./test_azure_cdc.sh parquet
```

**Creates changefeed with:**
- Format: `parquet`
- Compression: `gzip`
- Path: `parquet-cdc/`
- File extension: `.parquet`

### Test with JSON Format

```bash
cd sources/cockroachdb/scripts
./test_azure_cdc.sh json
```

**Creates changefeed with:**
- Format: `json`
- Envelope: `wrapped`
- Path: `json-cdc/`
- File extension: `.ndjson`
- Option: `split_column_families`

---

## What the Script Does

### 1. **Check Existing Changefeeds**
- Finds existing changefeeds for `usertable`
- Cancels any with errors
- Reuses healthy ones (skips creation)

### 2. **Create Changefeed** (if needed)
- Creates changefeed with specified format
- Writes to Azure Blob Storage
- Uses format-specific path prefix

### 3. **Run Workload**
- Executes YCSB workload for 10 seconds
- Generates CDC events

### 4. **Verify Azure Storage**
- Lists files in Azure
- Downloads sample file
- Shows file contents/info

### 5. **Analyze Statistics**
- Counts snapshot rows
- Counts INSERT/UPDATE/DELETE operations
- Provides detailed CDC statistics
- Uses `analyze_changefeed_stats.py`

### 6. **Save State**
- Saves changefeed Job ID
- Updates environment file
- Provides reuse instructions

---

## Output Paths

| Format | Path Prefix | File Extension | Compression |
|--------|-------------|----------------|-------------|
| **Parquet** | `parquet-cdc/` | `.parquet` | gzip |
| **JSON** | `json-cdc/` | `.ndjson` | none |

---

## File Structure

### Parquet Format
```
changefeed-events/
└── parquet-cdc/
    ├── 20251222174530...usertable.parquet
    ├── 20251222174531...usertable.parquet
    └── 20251222174532.RESOLVED
```

### JSON Format
```
changefeed-events/
└── json-cdc/
    ├── 20251222174530...usertable+fam_0_ycsb_key.ndjson
    ├── 20251222174530...usertable+fam_1_field0.ndjson
    ├── ...
    └── 20251222174532.RESOLVED
```

---

## Integration with Connector

### Python Notebook (`cockroachdb.ipynb`)

The notebook automatically uses the correct path based on format:

```python
credentials["azure_parquet_mode"] = {
    "azure_account_name": "...",
    "azure_account_key": "...",
    "azure_container": "changefeed-events",
    "azure_path_prefix": "parquet-cdc"  # Matches script default
}
```

### Databricks Pipeline

Update pipeline configuration:

```python
spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.schemaLocation", checkpoint_path) \
    .load("wasbs://changefeed-events@account.blob.core.windows.net/parquet-cdc/")
```

---

## Key Features

### 1. **Idempotent**
- Detects existing healthy changefeeds
- Skips creation if one already exists
- Still runs workload and verification

### 2. **Error Recovery**
- Automatically cancels errored changefeeds
- Polls for up to 60 seconds to confirm cancellation
- Creates new changefeed if needed

### 3. **Format Flexibility**
- Easy toggle between Parquet and JSON
- Separate paths prevent conflicts
- Format-specific verification

### 4. **Comprehensive Testing**
- Creates/reuses changefeed
- Runs workload
- Verifies file creation
- Downloads and inspects sample

---

## Sample Output

### Parquet Mode

```bash
$ ./test_azure_cdc.sh parquet

🧪 Testing CockroachDB → Azure Blob CDC
========================================

Format: PARQUET

Configuration:
  Format: parquet
  Account: cockroachcdc1766424661
  Container: changefeed-events
  Path: parquet-cdc
  URI: azure://changefeed-events/parquet-cdc?...

Step 1: Checking for existing changefeeds...
  ✅ No existing changefeeds

Step 2: Creating changefeed with correct Azure URI...
  ✅ Changefeed created: Job 1134937095266795521

Step 3: Waiting 10 seconds for changefeed to start...

Step 4: Checking changefeed status...
  Status: running
  ✅ Changefeed is running normally

Step 5: Running YCSB workload...
_elapsed___errors_____ops(total)___ops/sec(cum)__avg(ms)__p50(ms)__p95(ms)__p99(ms)_pMax(ms)__result
   10.0s        0           3470          347.0     68.9     67.1     96.5    117.4    151.0  

Step 6: Waiting 20 seconds for events to be written...

Step 7: Checking Azure Blob Storage...
  ✅ Found 15 parquet file(s)!

  Event files:
  [table showing .parquet files]
  
  📥 Downloading sample: parquet-cdc/20251222...usertable.parquet
  ✅ Parquet file downloaded
  📊 File size: 127K

  💡 To inspect Parquet file:
     python3 -c 'import pandas as pd; df = pd.read_parquet("/tmp/azure_sample_events.parquet"); print(df.head())'

Step 8: Analyzing changefeed statistics...

📊 Analyzing PARQUET changefeed files...
   Account: cockroachcdc1766424661
   Container: changefeed-events
   Prefix: parquet-cdc

✅ Found 15 parquet file(s)

  [1/15] Analyzing: 20251222...usertable.parquet... ✅
  [2/15] Analyzing: 20251222...usertable.parquet... ✅
  ...

================================================================================
📊 CHANGEFEED STATISTICS
================================================================================

  📸 Snapshot Rows:    1,000
  ➕ INSERT Operations: 1,735
  ✏️  UPDATE Operations: 1,735
  ➖ DELETE Operations: 0

  📈 Total Events:      4,470

================================================================================

✅ Azure CDC Test Complete!
```

### JSON Mode

```bash
$ ./test_azure_cdc.sh json

Format: JSON

Configuration:
  Format: json
  Path: json-cdc
  
[similar output with .ndjson files]

Step 7: Checking Azure Blob Storage...
  ✅ Found 11 json file(s)!
  
  📥 Downloading sample: json-cdc/...usertable+fam_0_ycsb_key.ndjson
  📊 Events in file: 10000
  
  🔍 First event:
  {
    "ycsb_key": "user1234567890",
    "updated": "1766426182.439985462,0"
  }
```

---

## Troubleshooting

### No Files Found

**Issue**: Step 7 shows "⚠️ No event files found yet"

**Solutions**:
1. Wait longer (changefeed may be flushing)
2. Check changefeed status for errors
3. Verify Azure credentials
4. Check the correct path prefix

### Format Mismatch

**Issue**: Connector can't find files

**Solution**: Ensure connector `azure_path_prefix` matches script format:
- Parquet: `parquet-cdc`
- JSON: `json-cdc`

### Existing Changefeed Wrong Format

**Issue**: Existing changefeed is JSON but you want Parquet

**Solution**: Cancel the existing changefeed first:
```bash
cd sources/cockroachdb/scripts
./cancel_job.sh <JOB_ID>
./test_azure_cdc.sh parquet
```

---

## Environment Variables

**Required** (loaded from `.env` files):
- `COCKROACHDB_URL` - CockroachDB connection string
- `AZURE_STORAGE_ACCOUNT` - Azure storage account name
- `AZURE_STORAGE_KEY` - Azure storage access key
- `AZURE_STORAGE_CONTAINER` - Container name (usually `changefeed-events`)

**Set by script**:
- `CHANGEFEED_FORMAT` - Format being used (`parquet` or `json`)
- `CHANGEFEED_JOB_ID` - Created changefeed job ID
- `AZURE_URI` - Full Azure URI with credentials

---

## Related Files

| File | Purpose |
|------|---------|
| `setup_azure_blob_for_cdc.sh` | Create Azure storage resources |
| `cancel_job.sh` | Cancel a specific changefeed job |
| `cancel_changefeed_job.py` | Shared cancel logic (imported) |
| `analyze_changefeed_stats.py` | Analyze changefeed statistics |
| `ANALYZE_CHANGEFEED_STATS.md` | Documentation for stats analyzer |
| `cockroachdb_cdc_azure.env` | Azure credentials |
| `cockroachdb_cockroachcloud.env` | CockroachDB credentials |

---

**Date**: December 22, 2025  
**Status**: ✅ Complete - Dual format support with Parquet default

