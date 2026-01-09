# CockroachDB CDC Credentials

This directory contains **credential files that should NEVER be committed to git**.

## Setup

### 1. Create Azure Config File

```bash
cp cockroachdb_cdc_azure.json.template cockroachdb_cdc_azure.json
```

Then edit `cockroachdb_cdc_azure.json` with your actual Azure credentials:

```json
{
  "azure_storage_account": "your-actual-storage-account",
  "azure_storage_key": "your-actual-storage-key-here",
  "azure_storage_container": "changefeed-events"
}
```

### 2. Create CockroachDB Credentials File

Create `cockroachdb_credentials.json`:

```json
{
  "cockroachdb_url": "postgresql://username:password@your-cluster.cockroachlabs.cloud:26257/database?sslmode=require"
}
```

### 3. (Optional) Create Pipeline Config

Create `cockroachdb_pipelines.json` for Unity Catalog volume paths:

```json
{
  "catalog": "main",
  "schema": "your_schema",
  "volume": "parquet_files"
}
```

## Files

- `cockroachdb_cdc_azure.json.template` - Template for Azure credentials (safe to commit)
- `cockroachdb_cdc_azure.json` - **YOUR ACTUAL CREDENTIALS** (gitignored, NEVER commit!)
- `cockroachdb_credentials.json` - **YOUR COCKROACHDB CREDENTIALS** (gitignored, NEVER commit!)
- `cockroachdb_pipelines.json` - Pipeline config (gitignored)

## Automatic Setup

You can also run the setup script to generate these files automatically:

```bash
cd sources/cockroachdb/scripts
./01_azure_storage.sh
```

This will:
1. Create Azure storage account
2. Create container
3. Generate `cockroachdb_cdc_azure.json` with your credentials

## Security

⚠️ **IMPORTANT:** Never commit files in this directory except for:
- `README.md` (this file)
- `*.template` files
- Example/documentation files

All actual credential files are automatically gitignored.
