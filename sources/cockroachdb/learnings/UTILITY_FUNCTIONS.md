# CockroachDB Utility Functions Reference

This document describes the consolidated utility functions available in `cockroachdb.py` for working with CockroachDB changefeeds.

## Overview

All Python helper code has been consolidated into `cockroachdb.py` to improve maintainability. The `changefeed_helper.py` CLI script is a thin wrapper around these utility functions, providing both changefeed operations and file analysis capabilities.

**Note**: `analyze_changefeed_stats.py` has been merged into `changefeed_helper.py` as the `analyze-files` command. A backward-compatibility shim is maintained for existing scripts.

## Benefits

- ✅ **Single Source of Truth** - Fix once, works everywhere
- ✅ **Better Testing** - Can unit test functions independently
- ✅ **Type Safety** - IDE autocomplete and type hints
- ✅ **Reusability** - Import from any Python script or notebook
- ✅ **Less Duplication** - Documentation references code, not duplicates
- ✅ **Easier Debugging** - Stack traces point to one location

## Available Utility Functions

### 1. Load Configuration

```python
from cockroachdb import load_crdb_config

# Load CockroachDB credentials
config = load_crdb_config('.env/cockroachdb_credentials.json')
print(config['cockroachdb_url'])

# Load Azure credentials
azure_config = load_crdb_config('.env/cockroachdb_cdc_azure.json')
print(azure_config['azure_storage_account'])
```

### 2. Create Connector

```python
from cockroachdb import load_crdb_config, create_connector

# Create connector from configuration
crdb_config = load_crdb_config('.env/cockroachdb_credentials.json')
connector = create_connector(crdb_config, catalog='ecommerce', schema='public')

# With Azure credentials for changefeed mode
azure_config = load_crdb_config('.env/cockroachdb_cdc_azure.json')
connector = create_connector(
    crdb_config, 
    catalog='ecommerce', 
    schema='public',
    azure_config=azure_config
)

# Get table row count
count = connector.get_table_row_count('orders')
print(f"Orders table has {count} rows")
```

### 3. Analyze Azure Changefeed Files

```python
from cockroachdb import load_crdb_config, analyze_azure_changefeed_files

# Load Azure credentials
azure_config = load_crdb_config('.env/cockroachdb_cdc_azure.json')

# Analyze Parquet changefeed files
stats = analyze_azure_changefeed_files(
    account_name=azure_config['azure_storage_account'],
    account_key=azure_config['azure_storage_key'],
    container_name=azure_config['azure_storage_container'],
    path_prefix='parquet-cdc',
    format_type='parquet',
    debug=False
)

print(f"Snapshot: {stats['snapshot']:,}")
print(f"Inserts:  {stats['insert']:,}")
print(f"Updates:  {stats['update']:,}")
print(f"Deletes:  {stats['delete']:,}")
print(f"Files:    {stats['file_count']}")
if 'unique_keys' in stats:
    print(f"Unique keys: {stats['unique_keys']:,}")
```

### 4. Analyze Individual Files

```python
from cockroachdb import analyze_json_changefeed_file, analyze_parquet_changefeed_file
from azure.storage.blob import BlobServiceClient

# Connect to Azure
connection_string = f"DefaultEndpointsProtocol=https;AccountName={account};AccountKey={key};EndpointSuffix=core.windows.net"
blob_service = BlobServiceClient.from_connection_string(connection_string)

# Analyze single JSON file
stats = analyze_json_changefeed_file(blob_service, 'container', 'path/to/file.ndjson')
print(f"File stats: {stats}")

# Analyze single Parquet file
stats = analyze_parquet_changefeed_file(blob_service, 'container', 'path/to/file.parquet', debug=True)
print(f"File stats: {stats}")
```

## Complete Example: Databricks Notebook

```python
# Databricks notebook cells

# Cell 1: Import utilities
from cockroachdb import load_crdb_config, create_connector, analyze_azure_changefeed_files

# Cell 2: Load credentials from Volume
crdb_config = load_crdb_config('/Volumes/main/default/lakeflow/.env/cockroachdb_credentials.json')
azure_config = load_crdb_config('/Volumes/main/default/lakeflow/.env/cockroachdb_cdc_azure.json')

# Cell 3: Create connector
connector = create_connector(
    crdb_config, 
    catalog='ecommerce', 
    schema='public',
    azure_config=azure_config
)

# Cell 4: Check changefeed status
changefeeds = connector.find_changefeeds_for_table('orders')
for cf in changefeeds:
    print(f"Job {cf['job_id']}: {cf['status']}")
    if cf['has_errors']:
        print(f"  ⚠️  Has errors!")

# Cell 5: Analyze changefeed statistics
stats = analyze_azure_changefeed_files(
    account_name=azure_config['azure_storage_account'],
    account_key=azure_config['azure_storage_key'],
    container_name=azure_config['azure_storage_container'],
    path_prefix='parquet/ecommerce/public/orders',
    format_type='parquet'
)

display(stats)
```

## CLI Scripts (Still Available)

The CLI script is a thin wrapper around these utility functions:

### changefeed_helper.py

**Changefeed Operations:**

```bash
# Find changefeeds
python3 changefeed_helper.py find-changefeeds \
    --table usertable \
    --json .env/cockroachdb_credentials.json

# Check status
python3 changefeed_helper.py check-status \
    --job-id 123456 \
    --json .env/cockroachdb_credentials.json

# Create changefeed
python3 changefeed_helper.py create-changefeed \
    --table orders \
    --azure-uri "azure://container/path?AZURE_ACCOUNT_NAME=...&AZURE_ACCOUNT_KEY=..." \
    --format parquet \
    --json .env/cockroachdb_credentials.json

# Cancel changefeed
python3 changefeed_helper.py cancel-changefeed \
    --job-id 123456 \
    --json .env/cockroachdb_credentials.json

# Get row count
python3 changefeed_helper.py get-row-count \
    --table orders \
    --json .env/cockroachdb_credentials.json

# Execute SQL
python3 changefeed_helper.py execute-sql \
    --sql "SELECT COUNT(*) FROM orders WHERE status = 'pending'" \
    --json .env/cockroachdb_credentials.json

# Get latest job
python3 changefeed_helper.py get-latest-job \
    --table orders \
    --json .env/cockroachdb_credentials.json
```

**File Analysis:**

```bash
# Analyze changefeed files in Azure Blob Storage
python3 changefeed_helper.py analyze-files \
    --format parquet \
    --account cockroachcdc1766424661 \
    --key "storage_account_key" \
    --container changefeed-events \
    --prefix parquet-cdc

# With debug output
python3 changefeed_helper.py analyze-files \
    --format json \
    --account cockroachcdc1766424661 \
    --key "storage_account_key" \
    --container changefeed-events \
    --prefix json-cdc \
    --debug

# Suppress JSON output (for cleaner terminal display)
python3 changefeed_helper.py analyze-files \
    --format parquet \
    --account cockroachcdc1766424661 \
    --key "storage_account_key" \
    --container changefeed-events \
    --no-json
```

### analyze_changefeed_stats.py (Deprecated - Backward Compatibility)

**⚠️  This script has been merged into `changefeed_helper.py analyze-files`**

A backward-compatibility shim is maintained for existing scripts:

```bash
# Old style (still works, but deprecated)
export AZURE_STORAGE_ACCOUNT=cockroachcdc1766424661
export AZURE_STORAGE_KEY=...
python3 analyze_changefeed_stats.py parquet

# Use this instead:
python3 changefeed_helper.py analyze-files \
    --format parquet \
    --account $AZURE_STORAGE_ACCOUNT \
    --key $AZURE_STORAGE_KEY \
    --container changefeed-events
```

## Migration Guide

If you had inline Python in bash scripts or notebooks, here's how to migrate:

### Before (Inline Python in Bash)

```bash
# Old approach - inline Python
RESULT=$(python3 << 'EOF'
import json
import sys
sys.path.insert(0, '.')
from cockroachdb import LakeflowConnect

with open('.env/cockroachdb_credentials.json') as f:
    config = json.load(f)

# Parse URL manually...
url = config['cockroachdb_url']
# ...lots of parsing code...

connector = LakeflowConnect({'token': token, 'base_url': base_url})
print(connector.get_table_row_count('orders'))
EOF
)
```

### After (Using Utility Functions)

```bash
# New approach - using changefeed_helper.py
RESULT=$(python3 changefeed_helper.py get-row-count \
    --table orders \
    --json .env/cockroachdb_credentials.json)
```

Or if you need programmatic access in your own Python script:

```python
from cockroachdb import load_crdb_config, create_connector

config = load_crdb_config('.env/cockroachdb_credentials.json')
connector = create_connector(config)
print(connector.get_table_row_count('orders'))
```

## Documentation Best Practices

In learning docs like `CDC_TEST_MATRIX_RESULTS.md`, reference the library:

```markdown
### Analysis

Use the `analyze_azure_changefeed_files` utility function:

```python
from cockroachdb import analyze_azure_changefeed_files, load_crdb_config

azure_config = load_crdb_config('.env/cockroachdb_cdc_azure.json')
stats = analyze_azure_changefeed_files(
    azure_config['azure_storage_account'],
    azure_config['azure_storage_key'],
    azure_config['azure_storage_container'],
    'parquet-cdc',
    format_type='parquet'
)
```

Or use the CLI wrapper:

```bash
./analyze_changefeed_stats.py parquet
```
```

This way, documentation stays accurate by reference instead of duplicating code!

## Testing

All utility functions can be imported and tested:

```bash
# Test imports
cd sources/cockroachdb
python3 -c "from cockroachdb import load_crdb_config, create_connector, analyze_azure_changefeed_files; print('✅ Success')"

# Test CLI scripts
cd sources/cockroachdb/scripts
python3 changefeed_helper.py --help
python3 analyze_changefeed_stats.py
```

## Related Files

- **`cockroachdb.py`** - Main library with all utility functions
- **`scripts/changefeed_helper.py`** - CLI wrapper for changefeed operations
- **`scripts/analyze_changefeed_stats.py`** - CLI wrapper for analyzing files
- **Learning docs** - Reference these utilities instead of duplicating code

