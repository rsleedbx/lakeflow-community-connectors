# Changefeed Statistics Analyzer

## Overview

`analyze_changefeed_stats.py` is a reusable Python script that analyzes CockroachDB changefeed files from Azure Blob Storage and provides detailed statistics about:
- **Snapshot rows** (initial table scan)
- **INSERT operations** (new CDC rows)
- **UPDATE operations** (modified CDC rows)
- **DELETE operations** (removed CDC rows)

## Features

✅ Supports both **Parquet** and **JSON** (NDJSON) formats  
✅ Analyzes multiple files in a single run  
✅ Provides human-readable output + machine-parseable JSON  
✅ Reusable across different changefeeds and tests  

## Usage

### Standalone Usage

#### Option 1: Use Environment Variables (Recommended)

```bash
# Load credentials from .env file
source sources/cockroachdb/.env/cockroachdb_cdc_azure.env

# Run analyzer - it will auto-detect everything
python3 analyze_changefeed_stats.py

# Or specify format only
python3 analyze_changefeed_stats.py parquet
python3 analyze_changefeed_stats.py json
```

#### Option 2: Explicit Arguments

```bash
# Set Azure storage key
export AZURE_STORAGE_KEY="your-storage-key"

# Analyze Parquet changefeed
python3 analyze_changefeed_stats.py \
    parquet \
    cockroachcdc1766424661 \
    changefeed-events \
    parquet-cdc

# Analyze JSON changefeed
python3 analyze_changefeed_stats.py \
    json \
    cockroachcdc1766424661 \
    changefeed-events \
    json-cdc
```

### Integrated with test_azure_cdc.sh

The script is automatically called at the end of `test_azure_cdc.sh`:

```bash
# Run test with Parquet format (default)
./test_azure_cdc.sh

# Run test with JSON format
./test_azure_cdc.sh json
```

## Output Example

```
📊 Analyzing PARQUET changefeed files...
   Account: cockroachcdc1766424661
   Container: changefeed-events
   Prefix: parquet-cdc

✅ Found 3 parquet file(s)

  [1/3] Analyzing: 2023-12-22T10-30-45-snapshot.parquet... ✅
  [2/3] Analyzing: 2023-12-22T10-31-00-cdc.parquet... ✅
  [3/3] Analyzing: 2023-12-22T10-31-15-cdc.parquet... ✅

================================================================================
📊 CHANGEFEED STATISTICS
================================================================================

  📸 Snapshot Rows:    1,000
  ➕ INSERT Operations: 50
  ✏️  UPDATE Operations: 30
  ➖ DELETE Operations: 10

  📈 Total Events:      1,090

================================================================================

JSON_STATS={"snapshot": 1000, "insert": 50, "update": 30, "delete": 10}
```

## Arguments

```
analyze_changefeed_stats.py [format] [account_name] [container] [path_prefix]

Arguments (all optional if environment variables are set):
  format        : parquet or json (default: $CHANGEFEED_FORMAT or 'parquet')
  account_name  : Azure storage account name (default: $AZURE_STORAGE_ACCOUNT)
  container     : Azure storage container name (default: $AZURE_STORAGE_CONTAINER or 'changefeed-events')
  path_prefix   : Path prefix (default: auto-detected as 'parquet-cdc' or 'json-cdc')

Environment variables:
  AZURE_STORAGE_ACCOUNT - Storage account name (required if not passed as argument)
  AZURE_STORAGE_KEY - Storage account key (always required)
  AZURE_STORAGE_CONTAINER - Container name (optional, default: 'changefeed-events')
  CHANGEFEED_FORMAT - Format (optional, default: 'parquet')
```

### Smart Defaults

The script automatically:
- Uses `CHANGEFEED_FORMAT` environment variable (or defaults to `parquet`)
- Auto-detects path prefix based on format:
  - `parquet` → `parquet-cdc/`
  - `json` → `json-cdc/`
- Uses `changefeed-events` as default container if not specified
- Reads all Azure credentials from environment variables

## Dependencies

### For JSON format:
- Python 3.6+ (built-in `json` module)
- `azure-storage-blob`

### For Parquet format:
- `azure-storage-blob`
- `pyarrow`
- `pandas`

Install dependencies:
```bash
pip install azure-storage-blob pyarrow pandas
```

## How It Works

### JSON (NDJSON) Format
1. Downloads each `.ndjson` file from Azure
2. Parses line-by-line as JSON
3. Analyzes `before` and `after` fields:
   - `after` only → Snapshot or INSERT
   - `after` + `before` → UPDATE
   - `before` only → DELETE

### Parquet Format

Supports multiple Parquet changefeed formats:

#### CockroachDB Native Format (with `split_column_families`)
1. Downloads each `.parquet` file from Azure
2. Reads using PyArrow/Pandas
3. Analyzes `__crdb__event_type` column:
   - `'c'` → Snapshot (create/initial scan)
   - `'i'` → INSERT
   - `'u'` → UPDATE
   - `'d'` → DELETE
4. Data columns are at top level (e.g., `ycsb_key`, `field0`)
5. **Note**: With `split_column_families`, each column family creates separate events, so event count = rows × column families

#### Debezium-style Format
1. Analyzes `before` and `after` columns:
   - `after` only → Snapshot or INSERT
   - `after` + `before` → UPDATE
   - `before` only → DELETE

## Integration Points

### In test_azure_cdc.sh
The script is called as **Step 8** after:
- Changefeed creation
- Workload execution (INSERT, UPDATE, DELETE)
- Azure Blob Storage verification

The shell script exports all required environment variables, so the Python script runs with no arguments:

```bash
# Export variables for Python script
export CHANGEFEED_FORMAT
export AZURE_STORAGE_ACCOUNT
export AZURE_STORAGE_KEY
export AZURE_STORAGE_CONTAINER

# Call analyzer - uses environment variables
python3 analyze_changefeed_stats.py
```

This provides immediate visibility into:
- How many rows were captured in the initial snapshot
- How many CDC operations were recorded
- Data distribution across INSERT/UPDATE/DELETE

### Parsing Output in Scripts
The script outputs a JSON line at the end for programmatic parsing:
```bash
JSON_STATS={"snapshot": 1000, "insert": 50, "update": 30, "delete": 10}
```

Extract in bash:
```bash
output=$(python3 analyze_changefeed_stats.py ...)
stats=$(echo "$output" | grep "^JSON_STATS=" | cut -d= -f2)
snapshot=$(echo "$stats" | jq -r '.snapshot')
```

## Understanding Event Counts

### CockroachDB with `split_column_families`

When using `split_column_families`, **each column family generates separate events**:

**Example**: Table with 10,000 rows and 11 column families
- **Snapshot events**: 10,000 rows × 11 families = **110,000 events**
- **UPDATE events**: 1 updated row × 11 families = **11 events**

This is **correct behavior**, not a bug! Each column family change is tracked independently.

### Without `split_column_families`

Without this option, event count matches row count:
- **Snapshot events**: 10,000 rows = **10,000 events**
- **UPDATE events**: 1 updated row = **1 event**

## Limitations

1. **Memory**: Loads entire files into memory
   - For very large files (>1GB), consider streaming
   - Current implementation works well for files < 500MB

2. **Performance**: Sequential processing
   - For many files, consider parallel processing with multiprocessing
   - Current implementation: ~1-2 seconds per file

## Future Enhancements

- [ ] Differentiate snapshot from INSERT using timestamps
- [ ] Add streaming support for large files
- [ ] Add parallel processing for multiple files
- [ ] Support for resolved timestamp tracking
- [ ] Support for other cloud providers (S3, GCS)
- [ ] CSV format support

## See Also

- `test_azure_cdc.sh` - End-to-end CDC test that uses this script
- `CHANGEFEED_EVENT_TYPES.md` - Documentation on changefeed event structure
- `TEST_AZURE_CDC_USAGE.md` - Usage guide for the test script

