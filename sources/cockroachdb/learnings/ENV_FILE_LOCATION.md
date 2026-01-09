# Environment File Location - UPDATED

## ✅ New Standardized Location

All environment files are now stored in:

```
sources/cockroachdb/.env/cockroachdb_cdc_azure.env
```

## Changes Made

### 1. `setup_azure_blob_for_cdc.sh`

**Now saves to**: `sources/cockroachdb/.env/cockroachdb_cdc_azure.env`

**Added**:
- Creates `.env` directory if it doesn't exist
- Includes both `COCKROACHDB_URL` and `COCKROACHDB_TOKEN`/`COCKROACHDB_BASE_URL` formats
- Uses Git root to find correct location

### 2. Scripts Updated

All scripts now load from the new location:

- ✅ `sources/cockroachdb/scripts/test_azure_cdc.sh`
- ✅ `sources/cockroachdb/scripts/create_azure_parquet_pipeline.sh`
- ✅ `sources/cockroachdb_s3/scripts/examine_changefeed_events.sh`
- ✅ `sources/cockroachdb_s3/scripts/diagnose_azure_connectivity.sh`
- ✅ `sources/cockroachdb_s3/scripts/check_changefeed_status.sh`
- ✅ `sources/cockroachdb_s3/scripts/diagnose_changefeed.sh`

### 3. Notebook Updated

The unit test notebook (`sources/cockroachdb/unittest/cockroachdb.ipynb`) already searches this location.

## Migration

If you have an existing `.env` file in the old location:

```bash
# Old location (deprecated)
sources/cockroachdb_s3/scripts/cockroachdb_cdc_azure.env

# Move to new location
mkdir -p sources/cockroachdb/.env
cp sources/cockroachdb_s3/scripts/cockroachdb_cdc_azure.env \
   sources/cockroachdb/.env/cockroachdb_cdc_azure.env
```

## Usage

### Shell Scripts

```bash
GIT_ROOT="$(git rev-parse --show-toplevel 2>/dev/null)"
ENV_FILE="$GIT_ROOT/sources/cockroachdb/.env/cockroachdb_cdc_azure.env"
source "$ENV_FILE"
```

### Python Notebooks

```python
import dotenv
import os
from git import Repo

repo = Repo('.', search_parent_directories=True)
git_root = repo.working_tree_dir
env_file = os.path.join(git_root, "sources/cockroachdb/.env/cockroachdb_cdc_azure.env")
dotenv.load_dotenv(env_file, override=True)
```

## Security

✅ The `.env` directory is in `.gitignore` - credentials are protected.

## File Format

```bash
# Azure Blob Storage for CockroachDB Changefeed
export AZURE_STORAGE_ACCOUNT=cockroachcdc1234567890
export AZURE_STORAGE_KEY=your-storage-key
export AZURE_STORAGE_CONTAINER=changefeed-events
export AZURE_CONNECTION_STRING=DefaultEndpointsProtocol=https;...
export CHANGEFEED_URI=azure-blob://changefeed-events?AZURE_ACCOUNT_NAME=...

# CockroachDB Connection (Full URL format)
export COCKROACHDB_URL="postgresql://user:password@host:26257/database?sslmode=require"

# CockroachDB Connection (GitHub-style token + base_url format - used by cockroachdb.py)
export COCKROACHDB_TOKEN="user:password"
export COCKROACHDB_BASE_URL="postgresql://host:26257/database?sslmode=require"
```

## Testing

After setup, verify:

```bash
# 1. File exists
ls -la sources/cockroachdb/.env/cockroachdb_cdc_azure.env

# 2. Can be sourced
source sources/cockroachdb/.env/cockroachdb_cdc_azure.env
echo $AZURE_STORAGE_ACCOUNT

# 3. Test script works
cd sources/cockroachdb/scripts
./test_azure_cdc.sh
```



