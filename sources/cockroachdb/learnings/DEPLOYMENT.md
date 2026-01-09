# CockroachDB Connector Deployment Guide

## Overview

This guide covers deploying the CockroachDB connector to Databricks and creating DLT (Delta Live Tables) pipelines for CDC ingestion.

## Deployment Scripts

### Prerequisites

- Databricks CLI configured and authenticated
- `jq` installed (for JSON parsing)
- Repository cloned locally
- CockroachDB connection details (host, port, database, user, password)

### Available Scripts

| Script | Purpose | Location |
|--------|---------|----------|
| `copydir.sh` | Deploy connector files to Databricks workspace | `sources/cockroachdb/scripts/` |
| `createpipeline.sh` | Create DLT pipeline via CLI | `sources/cockroachdb/scripts/` |
| `create_databricks_connection.sh` | Create Unity Catalog connection | `sources/cockroachdb/scripts/` |

## Step-by-Step Deployment

### Step 1: Create Unity Catalog Connection

Create a connection to your CockroachDB cluster:

```bash
cd sources/cockroachdb

# For CockroachCloud (with SSL):
./scripts/create_databricks_connection.sh \
  "postgresql://user:password@battle-walrus-11108.jxf.gcp-us-east1.cockroachlabs.cloud:26257/defaultdb?sslmode=verify-full" \
  "cockroachdb_connection"

# For local CockroachDB (no SSL):
./scripts/create_databricks_connection.sh \
  "postgresql://root@localhost:26257/mydb?sslmode=disable" \
  "local_crdb"
```

**Connection Parameters Parsed:**
- Host
- Port
- Database
- User
- Password
- SSL mode

The connection is created with `sourceName: "cockroachdb"` and all required options.

### Step 2: Deploy Connector to Workspace

Upload the connector files to your Databricks workspace:

```bash
# Navigate to repository root
cd /path/to/lakeflow-community-connectors

# Deploy connector
./sources/cockroachdb/scripts/copydir.sh
```

**What it does:**
1. Generates merged source file (`_generated_cockroachdb_python_source.py`)
2. Creates temp directory with required files:
   - `libs/` - Source loader, spec parser, utils
   - `sources/cockroachdb/` - Connector implementation
   - `pipeline/` - Ingestion pipeline framework
   - `ingest.py` - Pipeline configuration
3. Syncs files to `/Workspace/Users/<your-username>/cockroachdb/`
4. Cleans up temp directory

**Files uploaded:**
```
/Workspace/Users/<username>/cockroachdb/
├── ingest.py
├── libs/
│   ├── source_loader.py
│   ├── spec_parser.py
│   └── utils.py
├── pipeline/
│   └── ingestion_pipeline.py
└── sources/
    └── cockroachdb/
        ├── __init__.py
        ├── cockroachdb.py
        └── _generated_cockroachdb_python_source.py
```

### Step 3: Create DLT Pipeline

Create a DLT pipeline to ingest data:

```bash
# Use default connection and ingest all tables
./sources/cockroachdb/scripts/createpipeline.sh

# Use custom connection name
./sources/cockroachdb/scripts/createpipeline.sh my_crdb_connection

# Ingest specific tables only
./sources/cockroachdb/scripts/createpipeline.sh cockroachdb_connection "customers,orders,products"
```

**What it does:**
1. Checks for existing pipeline with same name
2. Deletes existing pipeline if found (to avoid conflicts)
3. Creates Unity Catalog schema (`main.<pipeline_name>`)
4. Creates new DLT pipeline with:
   - Name: `<username>_cockroachdb`
   - Catalog: `main`
   - Schema: `<username>_cockroachdb`
   - Serverless: `true`
   - Development mode: `true`
5. Saves pipeline ID and displays URL

**Pipeline Configuration:**
- `source_name`: "cockroachdb"
- `connection_name`: Your connection name
- `table_list`: Optional comma-separated list of tables

### Step 4: Start Pipeline

Start the pipeline to begin CDC ingestion:

```bash
# Pipeline ID is displayed after creation
databricks pipelines start-update <pipeline_id>

# Monitor progress
databricks pipelines get <pipeline_id> --output json | jq -r '.state'
```

## How It Works

### ingest.py - Pipeline Configuration

The `ingest.py` file dynamically configures the pipeline:

```python
# Reads from DLT pipeline configuration (via spark.conf)
connection_name = spark.conf.get("connection_name", "cockroachdb_connection")
table_list_str = spark.conf.get("table_list", "")  # Optional

# Discovers tables from connection
connector = LakeflowConnect({})
if table_list_str:
    all_tables = [t.strip() for t in table_list_str.split(",")]
else:
    all_tables = connector.list_tables()  # All tables

# Configures CDC for each table
default_table_config = {
    "initial_scan": "yes",       # Full scan + streaming
    "resolved_interval": "10s",   # Resolved timestamps frequency
}

# Builds and executes pipeline spec
pipeline_spec = {
    "connection_name": connection_name,
    "objects": [
        {
            "table": {
                "source_table": table_name,
                "table_configuration": default_table_config
            }
        }
        for table_name in all_tables
    ]
}
```

### CDC Configuration Options

Per-table options (set in `table_configuration`):

| Option | Description | Values | Default |
|--------|-------------|--------|---------|
| `initial_scan` | CDC mode | `"yes"` (full+stream), `"only"` (snapshot) | `"yes"` |
| `resolved_interval` | Resolved timestamp frequency | e.g., `"1s"`, `"10s"`, `"1m"` | `"10s"` |
| `batch_size` | Rows per batch | String number, e.g., `"100"` | `"1000"` |
| `split_column_families` | Multi-family tables | `"true"`, `"false"` | `"false"` |

**Example custom configuration:**

Edit `ingest.py` to customize per-table settings:

```python
# Custom configurations per table
table_configs = {
    "customers": {
        "initial_scan": "yes",
        "resolved_interval": "1s",  # Frequent updates
        "batch_size": "500"
    },
    "orders": {
        "initial_scan": "only",  # Snapshot only
        "batch_size": "1000"
    }
}

pipeline_spec = {
    "connection_name": connection_name,
    "objects": [
        {
            "table": {
                "source_table": table_name,
                "table_configuration": table_configs.get(table_name, default_table_config)
            }
        }
        for table_name in all_tables
    ]
}
```

## Pipeline Management

### Update Pipeline Configuration

To change tables or settings, recreate the pipeline:

```bash
# Delete and recreate with new configuration
./sources/cockroachdb/scripts/createpipeline.sh cockroachdb_connection "customers,orders"
```

### Redeploy Connector

After making code changes:

```bash
# 1. Redeploy connector files
./sources/cockroachdb/scripts/copydir.sh

# 2. Restart pipeline to pick up changes
databricks pipelines start-update <pipeline_id>
```

### View Pipeline

```bash
# Get pipeline details
databricks pipelines get <pipeline_id> --output json | jq

# View in browser
# URL: https://<workspace>.cloud.databricks.com/pipelines/<pipeline_id>
```

## Troubleshooting

### Connection Issues

**Error: Connection failed**

Check your connection URL and parameters:

```bash
# Test connection string parsing
python scripts/test_local.py \
  --url "postgresql://user:pass@host:port/db?sslmode=verify-full" \
  --no-data
```

**Error: Rangefeeds not enabled**

Enable rangefeeds on your CockroachDB cluster:

```sql
SET CLUSTER SETTING kv.rangefeed.enabled = true;
```

### Pipeline Failures

**Error: Connection not found**

Verify the connection exists:

```bash
databricks connections get cockroachdb_connection
```

**Error: Table not found**

Check available tables:

```bash
python scripts/test_local.py \
  --url "postgresql://..." \
  --no-data
```

This will list all tables in the database.

**Error: Files not found in workspace**

Redeploy the connector:

```bash
./sources/cockroachdb/scripts/copydir.sh
```

### Development Workflow

For rapid iteration:

```bash
# 1. Make code changes locally
vim sources/cockroachdb/cockroachdb.py

# 2. Deploy immediately
./sources/cockroachdb/scripts/copydir.sh

# 3. Restart pipeline
databricks pipelines start-update <pipeline_id>

# 4. Monitor logs in Databricks UI
# No git commit needed until validated!
```

## Advanced Configuration

### Multiple Connections

Support multiple CockroachDB clusters:

```bash
# Create connections
./scripts/create_databricks_connection.sh "postgresql://..." "prod_crdb"
./scripts/create_databricks_connection.sh "postgresql://..." "dev_crdb"

# Create pipelines
./scripts/createpipeline.sh prod_crdb "customers"
./scripts/createpipeline.sh dev_crdb "test_table"
```

### Custom Schema Mapping

Edit `ingest.py` to use custom target schemas:

```python
# Target different catalogs/schemas per table
pipeline_spec = {
    "connection_name": connection_name,
    "objects": [
        {
            "table": {
                "source_table": "customers",
                "destination_table": "bronze.raw_customers",  # Custom target
                "table_configuration": default_table_config
            }
        }
    ]
}
```

### Continuous vs Triggered

Change pipeline mode in `createpipeline.sh`:

```bash
# Continuous (always running)
"continuous": true,

# Triggered (manual start)
"continuous": false,
```

## Examples

### Example 1: Single Table CDC

```bash
# 1. Create connection
./scripts/create_databricks_connection.sh \
  "postgresql://user:pass@host:port/mydb?sslmode=require" \
  "my_connection"

# 2. Deploy connector
./scripts/copydir.sh

# 3. Create pipeline for one table
./scripts/createpipeline.sh my_connection "customers"

# 4. Start ingestion
databricks pipelines start-update <pipeline_id>
```

### Example 2: Multi-Table CDC

```bash
# All tables from connection
./scripts/createpipeline.sh my_connection

# Or specific tables
./scripts/createpipeline.sh my_connection "customers,orders,products,inventory"
```

### Example 3: Snapshot Only

Edit `ingest.py` before deploying:

```python
default_table_config = {
    "initial_scan": "only",  # Snapshot, don't stream
}
```

Then deploy and create pipeline normally.

## References

- **Scripts adapted from:** `nullsource-connector` branch (`sources/nullsource/README.md`)
- **Original implementation:** Nullsource connector deployment workflow
- **DLT Documentation:** [Delta Live Tables](https://docs.databricks.com/delta-live-tables/)
- **CLI Reference:** [Databricks CLI](https://docs.databricks.com/dev-tools/cli/)

## Summary

| Step | Command | Purpose |
|------|---------|---------|
| 1. Create connection | `./scripts/create_databricks_connection.sh "<url>" "name"` | Unity Catalog connection |
| 2. Deploy connector | `./scripts/copydir.sh` | Upload files to workspace |
| 3. Create pipeline | `./scripts/createpipeline.sh [connection] [tables]` | DLT pipeline |
| 4. Start ingestion | `databricks pipelines start-update <id>` | Begin CDC |

**Fast iteration workflow:**  
Edit code → `copydir.sh` → restart pipeline → validate → repeat

