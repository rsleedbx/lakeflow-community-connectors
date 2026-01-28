# CockroachDB Pipeline Configuration Examples

This directory contains example configurations for different deployment modes of the CockroachDB connector.

## Quick Start

1. **Copy an example config to `.env/` directory:**
   ```bash
   cp config_examples/volume_mode.json .env/cockroachdb_pipelines.json
   ```

2. **Edit the config to match your environment:**
   ```bash
   # Update pipeline_name, catalog, schema, etc.
   nano .env/cockroachdb_pipelines.json
   ```

3. **Deploy the pipeline:**
   ```bash
   cd scripts
   ./deploy_pipeline.sh
   ```

> **Note:** The deployment script automatically appends the mode as a suffix to both `pipeline_name` and `schema`.
> For example, if your config has `"pipeline_name": "my_cockroachdb"` and `"mode": "direct"`,
> the actual pipeline will be named `my_cockroachdb_direct` and the schema will be created as `catalog.my_schema_direct`.

## Available Modes

### 1. DIRECT Mode (`direct_mode.json`)

**Use case:** Testing and development with direct CockroachDB sinkless changefeed connection

**Configuration:**
- `crdb_config`: Path to JSON file containing CockroachDB connection details
- `table_list`: Comma-separated list of tables to sync

**Prerequisites:**
- CockroachDB cluster with network connectivity
- CockroachDB connection details (URL, credentials)
- Create a config file with connection details:

```json
{
  "cockroachdb_url": "postgresql://user:password@hostname:26257/defaultdb?sslmode=verify-full",
  "token": "optional_api_token_for_serverless",
  "base_url": "https://cockroachlabs.cloud"
}
```

**Example Pipeline Config:**
```json
{
  "pipeline_name": "my_cockroachdb",
  "mode": "direct",
  "catalog": "main",
  "schema": "my_schema",
  "crdb_config": ".env/cockroachdb_connection.json",
  "table_list": "usertable,orders"
}
```

**Important:** DIRECT mode connects directly to CockroachDB using sinkless changefeeds. It does NOT use Databricks PostgreSQL connections, as those don't support the required DataFrame API operations.

### 2. VOLUME Mode (`volume_mode.json`)

**Use case:** File-based testing with pre-synced CDC data in Unity Catalog Volumes

**Configuration:**
- `volume_name`: Name of the Unity Catalog Volume containing CDC files

**Prerequisites:**
- Unity Catalog Volume created: `/Volumes/{catalog}/{schema}/{volume_name}`
- Volume populated with CDC files (use `sync_azure_to_volume_compact.py`)
- Volume structure:
  ```
  /Volumes/catalog/schema/volume/
  ├── _metadata/schema.json (optional)
  ├── 2024-01-22_10-15-30/
  │   ├── snapshot_*.parquet
  │   └── cdc_*.parquet
  ```

**Example:**
```json
{
  "pipeline_name": "my_cockroachdb_volume",
  "mode": "volume",
  "catalog": "main",
  "schema": "my_schema",
  "volume_name": "cdc_files"
}
```

### 3. AZURE_PARQUET Mode (`azure_parquet_mode.json`)

**Use case:** Production CDC with Parquet format changefeeds in Azure Blob Storage

**Configuration:**
- `azure_config`: Path to Azure credentials JSON file
- `blob_prefix`: Path prefix in Azure container (e.g., `parquet/defaultdb/public/usertable`)

**Prerequisites:**
- Azure Blob Storage account with CDC files
- Azure credentials file (`.env/cockroachdb_cdc_azure.json`):
  ```json
  {
    "account_name": "mystorageaccount",
    "account_key": "base64_key==",
    "container_name": "cockroachdb-cdc",
    "base_url": "https://mystorageaccount.blob.core.windows.net"
  }
  ```

**Example:**
```json
{
  "pipeline_name": "my_cockroachdb_azure_parquet",
  "mode": "azure_parquet",
  "catalog": "main",
  "schema": "my_schema",
  "azure_config": "./.env/cockroachdb_cdc_azure.json",
  "blob_prefix": "parquet/defaultdb/public/usertable"
}
```

### 4. AZURE_JSON Mode (`azure_json_mode.json`)

**Use case:** Production CDC with JSON format changefeeds in Azure Blob Storage

**Configuration:**
- Same as AZURE_PARQUET but with `mode: "azure_json"`
- `blob_prefix` should point to JSON changefeed path

**Benefits:**
- Human-readable format
- Easier debugging
- Good for development/testing

**Example:**
```json
{
  "pipeline_name": "my_cockroachdb_azure_json",
  "mode": "azure_json",
  "catalog": "main",
  "schema": "my_schema",
  "azure_config": "./.env/cockroachdb_cdc_azure.json",
  "blob_prefix": "json/defaultdb/public/usertable"
}
```

### 5. AZURE_DUAL Mode (`azure_dual_mode.json`)

**Use case:** Reading both JSON and Parquet changefeeds from the same table

**Configuration:**
- `mode: "azure_dual"`
- Connector automatically detects and processes both formats
- Ensures deduplication across formats

**Use cases:**
- Format migration (JSON → Parquet)
- Redundancy for critical tables
- Testing/validation

**Example:**
```json
{
  "pipeline_name": "my_cockroachdb_azure_dual",
  "mode": "azure_dual",
  "catalog": "main",
  "schema": "my_schema",
  "azure_config": "./.env/cockroachdb_cdc_azure.json",
  "blob_prefix": "json/defaultdb/public/usertable"
}
```

## Deployment Options

### Basic Deployment
```bash
./deploy_pipeline.sh
```

### Force Delete Existing Pipeline (No Prompt)
```bash
./deploy_pipeline.sh --force
```

### Create Pipeline Without Starting
```bash
./deploy_pipeline.sh --no-start
```

### Use Alternate Config File
```bash
./deploy_pipeline.sh --config config_examples/direct_mode.json
```

### Combined Options
```bash
./deploy_pipeline.sh --force --no-start --config /path/to/custom_config.json
```

## Multi-Environment Setup

You can maintain multiple configs for different environments:

```bash
# Development
cp config_examples/direct_mode.json .env/cockroachdb_pipelines_dev.json

# Staging
cp config_examples/volume_mode.json .env/cockroachdb_pipelines_staging.json

# Production
cp config_examples/azure_parquet_mode.json .env/cockroachdb_pipelines_prod.json

# Deploy to specific environment
./deploy_pipeline.sh --config .env/cockroachdb_pipelines_prod.json
```

## Troubleshooting

### DIRECT Mode Issues

**Problem:** Connection not found
```
⚠️  Warning: Connection 'cockroachdb_connection' not found
```

**Solution:**
1. Create connection in Databricks:
   ```bash
   databricks connections create --json '{"name": "cockroachdb_connection", ...}'
   ```
2. Or use existing connection name from `databricks connections list`

### VOLUME Mode Issues

**Problem:** Cannot access volume
```
❌ Cannot access volume: /Volumes/main/my_schema/cdc_files
```

**Solution:**
1. Check volume exists: `databricks volumes list --catalog main --schema my_schema`
2. Verify permissions: Ensure you have READ permission
3. Authenticate: `databricks auth login`

**Problem:** No data files found
```
❌ No data files found in volume directories
```

**Solution:**
1. Populate volume with CDC files:
   ```bash
   python3 sync_azure_to_volume_compact.py \
     --catalog main \
     --schema my_schema \
     --volume cdc_files \
     --azure-config .env/cockroachdb_cdc_azure.json
   ```

### AZURE Mode Issues

**Problem:** Azure config not found
```
❌ Azure config file not found: ./.env/cockroachdb_cdc_azure.json
```

**Solution:**
1. Create Azure credentials file with correct format
2. Update `azure_config` path in pipeline config

## Best Practices

1. **Use DIRECT mode** for quick testing and development
2. **Use VOLUME mode** for repeatable testing with fixed datasets
3. **Use AZURE_PARQUET mode** for production (smaller files, faster)
4. **Use AZURE_JSON mode** for debugging or when human-readability is important
5. **Use AZURE_DUAL mode** during format migrations or for redundancy

## Related Scripts

- `deploy_pipeline.sh` - Main deployment script
- `copy_connector_to_workspace.sh` - Copy connector code to Databricks workspace
- `monitor_pipeline.sh` - Monitor pipeline execution
- `sync_azure_to_volume_compact.py` - Sync Azure CDC files to Unity Catalog Volume

## Automatic Mode Suffixes

**The deployment script automatically manages mode suffixes to avoid naming conflicts:**

### How It Works

1. **Strips any existing mode suffix** from `pipeline_name` and `schema`
2. **Appends the current mode suffix** based on `mode` field

| Mode | Suffix | Example |
|------|--------|---------|
| `direct` | `_direct` | `my_pipeline` → `my_pipeline_direct` |
| `volume` | `_volume` | `my_pipeline` → `my_pipeline_volume` |
| `azure_parquet` | `_azure_parquet` | `my_pipeline` → `my_pipeline_azure_parquet` |
| `azure_json` | `_azure_json` | `my_pipeline` → `my_pipeline_azure_json` |
| `azure_dual` | `_azure_dual` | `my_pipeline` → `my_pipeline_azure_dual` |

### Examples

**Config has base name:**
- Config: `"pipeline_name": "my_cockroachdb"`, `"mode": "direct"`
- Result: `my_cockroachdb_direct`

**Config has old mode suffix (normalized):**
- Config: `"pipeline_name": "my_cockroachdb_volume"`, `"mode": "direct"`
- Result: `my_cockroachdb_direct` (strips `_volume`, adds `_direct`)

**Config already has correct suffix (idempotent):**
- Config: `"pipeline_name": "my_cockroachdb_direct"`, `"mode": "direct"`
- Result: `my_cockroachdb_direct` (strips `_direct`, adds `_direct`)

**Both `pipeline_name` and `schema` get normalized:**
- Config: `"pipeline_name": "my_cockroachdb"`, `"schema": "robert_lee_cockroachdb"`, `"mode": "direct"`
- Result: Pipeline = `my_cockroachdb_direct`, Schema = `main.robert_lee_cockroachdb_direct`

### Why This Design?

1. **Reuse configs across modes:** Copy a config file and just change the `mode` field
2. **Avoid naming conflicts:** Different modes create separate pipelines/schemas
3. **Idempotent:** Re-running with same config produces same names
4. **Side-by-side deployments:** Test multiple modes simultaneously

**Example:** Test DIRECT and VOLUME modes simultaneously:
```bash
# Deploy DIRECT mode
./deploy_pipeline.sh --config config_examples/direct_mode.json
# Creates: my_cockroachdb_direct pipeline → main.my_schema_direct

# Deploy VOLUME mode
./deploy_pipeline.sh --config config_examples/volume_mode.json
# Creates: my_cockroachdb_volume pipeline → main.my_schema_volume
```

Both pipelines can coexist without conflicts! ✅

## Configuration Schema Reference

### Required Fields (All Modes)
- `pipeline_name` (string): Base name for the DLT pipeline (mode suffix will be appended automatically)
- `mode` (string): One of: `direct`, `volume`, `azure_parquet`, `azure_json`, `azure_dual`
- `catalog` (string): Unity Catalog catalog name
- `schema` (string): Base schema/database name in Unity Catalog (mode suffix will be appended automatically)

### Mode-Specific Fields

**DIRECT:**
- `crdb_config` (string, required): Path to CockroachDB connection config JSON file
- `table_list` (string, optional): Comma-separated table names (default: "usertable")
- Note: DIRECT mode uses sinkless changefeeds, connecting directly to CockroachDB (not via Databricks connections)

**VOLUME:**
- `volume_name` (string, required): Unity Catalog Volume name

**AZURE_* (all Azure modes):**
- `azure_config` (string, required): Path to Azure credentials JSON
- `blob_prefix` (string, required): Path prefix in Azure container

### Optional Fields
- `_comment` (string): Human-readable comment (ignored by script)
- `_description` (string): Description of the config (ignored by script)
- `_notes` (array): Additional notes (ignored by script)
