# Pipeline Specification Examples

This directory contains examples for creating data ingestion pipelines using Lakeflow Community Connectors.

## Overview

Lakeflow pipelines use the **Spark Declarative Pipeline (SDP)** framework to create streaming data ingestion workflows. Each pipeline:

1. **Registers** a source connector with Spark
2. **Configures** which tables to ingest and where to store them
3. **Creates** streaming tables with automatic change data capture (CDC) or append patterns

## Files

- **`example_ingest.py`** - Template for creating new pipelines (requires customization)
- **`example_sdp_pipeline.py`** - ✅ **Valid SDP** for Databricks DLT/Workflows (use this!)
- **`example_connector_pipeline.py`** - Helper script (not a valid SDP, for reference only)
- **`example_local_demo.py`** - Local demonstration of connector functionality (no Databricks required)

### Important: SDP vs Regular Python

**Valid SDP** (`example_sdp_pipeline.py`):
- ✅ Pipeline definitions at module level
- ✅ Works with Databricks DLT and Workflows
- ✅ SDP decorators (`@sdp.append_flow`, `sdp.create_streaming_table`)
- ✅ Can be deployed as a Databricks pipeline

**Regular Python** (`example_connector_pipeline.py`):
- ❌ Uses `if __name__ == "__main__"`
- ❌ Not recognized as SDP by Databricks
- ℹ️ Useful for understanding the flow, but not for deployment

## Quick Start: Using the Example Connector

### Step 1: Generate the Deployable Source

The connector needs to be merged into a single file before use:

```bash
python3 scripts/merge_python_source.py example
```

This creates: `sources/example/_generated_example_python_source.py`

### Step 2: Test Locally (Optional)

Run the local demo to see how the connector works:

```bash
source .venv/bin/activate  # Activate virtual environment
python pipeline-spec/example_local_demo.py
```

**Output:**
```
======================================================================
Example Connector Demo
======================================================================

1. Initializing connector with options...
   ✅ Connector initialized

2. Listing available tables...
   ✅ Found 2 tables: ['my_table', 'your_table']

3. Getting table schemas...
   📋 my_table:
      - id: bigint
      - name: string
   📋 your_table:
      - key: string
      - value: string

5. Reading sample data...
   📁 Reading from my_table...
      ✅ Retrieved 5 records
      Record 1: {'id': 0, 'name': 'Name_4828'}
      Record 2: {'id': 1, 'name': 'Name_6083'}
      ...
```

### Step 3: Create a Unity Catalog Connection (Databricks)

In your Databricks workspace, create a Unity Catalog connection:

**Via UI:**
1. Go to **Data** → **Connections**
2. Click **Create Connection**
3. Choose **Lakeflow Community Connector**
4. Select **example** connector
5. Provide credentials:
   ```json
   {
     "user": "example_user",
     "password": "example_password",
     "token": "example_token"
   }
   ```
6. Name it: `example_connection`

**Via SQL:**
```sql
CREATE CONNECTION example_connection
TYPE lakeflow_connect
OPTIONS (
  'databricks.connection.name' = 'example_connection',
  'user' = 'example_user',
  'password' = 'example_password',
  'token' = 'example_token'
);
```

**Via Databricks CLI:**
```bash
databricks connections create --json '{
  "name": "example_connection",
  "connection_type": "GENERIC_LAKEFLOW_CONNECT",
  "options": {
    "sourceName": "example",
    "user": "example_user",
    "password": "example_password",
    "token": "example_token"
  }
}'
```

### Step 4: Create and Run the Pipeline

Use the provided pipeline example:

```python
# example_connector_pipeline.py

from pipeline.ingestion_pipeline import ingest
from libs.source_loader import get_register_function

source_name = "example"

pipeline_spec = {
    "connection_name": "example_connection",
    "objects": [
        {
            "table": {
                "source_table": "my_table",
                "destination_catalog": "main",
                "destination_schema": "default",
                "destination_table": "my_table",
                "table_configuration": {
                    "num_rows": 100,  # Generate 100 rows
                },
            }
        },
        {
            "table": {
                "source_table": "your_table",
                "destination_catalog": "main",
                "destination_schema": "default",
                "destination_table": "your_table",
                "table_configuration": {
                    "num_rows": 50,
                },
            }
        },
    ],
}

# Register and run
register_lakeflow_source = get_register_function(source_name)
register_lakeflow_source(spark)
ingest(spark, pipeline_spec)
```

**Run in Databricks:**

**Option 1: Via Databricks CLI (Create DLT Pipeline)**

```bash
# 1. Upload required files to your workspace
USER_PATH="/Workspace/Users/$(databricks current-user me --output json | jq -r '.userName')"
PROJECT_PATH="$USER_PATH/example_connector"

# Create temp directory with only required files
TEMP_DIR=$(mktemp -d)
mkdir -p "$TEMP_DIR/libs" "$TEMP_DIR/sources/example" "$TEMP_DIR/pipeline-spec"
cp libs/__init__.py libs/source_loader.py "$TEMP_DIR/libs/"
cp sources/__init__.py "$TEMP_DIR/sources/"
cp sources/example/__init__.py sources/example/_generated_example_python_source.py "$TEMP_DIR/sources/example/"
cp pipeline-spec/example_sdp_pipeline.py "$TEMP_DIR/pipeline-spec/"

# Sync to workspace
databricks sync "$TEMP_DIR" "$PROJECT_PATH" --full
rm -rf "$TEMP_DIR"

echo "Files uploaded to: $PROJECT_PATH"

# 2. Create the DLT pipeline
databricks pipelines create --json '{
  "name": "example_connector_pipeline",
  "libraries": [
    {
      "file": {
        "path": "'"$PROJECT_PATH"'/pipeline-spec/example_sdp_pipeline.py"
      }
    }
  ],
  "target": "example_connector_dev",
  "development": true,
  "channel": "PREVIEW",
  "catalog": "main",
  "serverless": true,
  "storage": "'"$PROJECT_PATH"'"
}'

# 3. Start the pipeline (replace PIPELINE_ID with output from create command)
# databricks pipelines start PIPELINE_ID

# View pipeline in UI
WORKSPACE_URL=$(databricks auth env --output json | jq -r '.env.DATABRICKS_HOST')
echo "View pipelines at: ${WORKSPACE_URL}/pipelines"
```

To update an existing pipeline:
```bash
# Get pipeline ID
PIPELINE_ID=$(databricks pipelines list --output json | jq -r '.statuses[] | select(.name=="example_connector_pipeline") | .pipeline_id')

# Update the pipeline configuration
databricks pipelines update "$PIPELINE_ID" --json '{
  "name": "example_connector_pipeline",
  "libraries": [
    {
      "file": {
        "path": "'"$PROJECT_PATH"'/pipeline-spec/example_sdp_pipeline.py"
      }
    }
  ],
  "target": "example_connector_dev",
  "development": true,
  "channel": "PREVIEW",
  "catalog": "main",
  "serverless": true,
  "storage": "'"$PROJECT_PATH"'"
}'

# Start the updated pipeline
databricks pipelines start "$PIPELINE_ID"
```

**Option 2: Via Databricks CLI (Direct Job Submit)**

```bash
# Upload files (same as Option 1 above)

# Run immediately with jobs submit (serverless)
databricks jobs submit --json '{
  "run_name": "Example SDP Pipeline Run",
  "tasks": [{
    "task_key": "run_pipeline",
    "spark_python_task": {
      "python_file": "'"$PROJECT_PATH"'/pipeline-spec/example_sdp_pipeline.py"
    },
    "compute": {
      "spec": {
        "kind": "serverless_compute"
      }
    }
  }]
}'
```

**Option 3: Via Notebook**
1. Upload project files to your workspace (using commands from Option 1)
2. Create a notebook and run:
   ```python
   %run /Workspace/Users/<your-username>/example_connector/pipeline-spec/example_sdp_pipeline
   ```

### What Gets Created

The pipeline automatically creates:

**For `my_table` (append mode):**
```python
# Streaming table
CREATE STREAMING TABLE main.default.my_table

# Append flow (no deduplication)
@append_flow(target="my_table")
```

**Schema:**
- `id` (bigint) - Primary key
- `name` (string) - Random name

**For `your_table` (append mode):**
```python
CREATE STREAMING TABLE main.default.your_table

@append_flow(target="your_table")
```

**Schema:**
- `key` (string) - Primary key
- `value` (string) - Random value

## Pipeline Configuration Options

### Connection Configuration

```python
pipeline_spec = {
    "connection_name": "example_connection",  # UC connection name
    "objects": [...]
}
```

### Table Configuration

Each table in `objects` can specify:

```python
{
    "table": {
        # Required
        "source_table": "my_table",        # Source table name
        
        # Optional - Destination
        "destination_catalog": "main",      # Target catalog (default: pipeline default)
        "destination_schema": "default",    # Target schema (default: pipeline default)
        "destination_table": "my_table",    # Target table (default: source_table)
        
        # Optional - Behavior
        "table_configuration": {
            "scd_type": "SCD_TYPE_1",       # SCD_TYPE_1 (default), SCD_TYPE_2, or APPEND_ONLY
            "primary_keys": ["id"],         # Override default primary keys
            
            # Connector-specific options
            "num_rows": 100,                # Example: number of rows to generate
        },
    }
}
```

### Ingestion Types

The example connector uses **append** mode for both tables:

- **`append`** - New records are appended without deduplication
  - No primary keys required
  - Best for event streams or logs
  - Lower overhead

Other connectors may support:

- **`cdc`** (Change Data Capture) - Incremental updates based on a cursor field
  - Requires: `primary_keys` and `cursor_field` (e.g., `updated_at`)
  - Deduplicates by primary key
  - Supports SCD Type 1 or Type 2

- **`snapshot`** - Full table refresh
  - Requires: `primary_keys`
  - Replaces entire dataset each run
  - No incremental cursor needed

## Using Other Connectors

To use GitHub, HubSpot, Stripe, or Zendesk:

1. **Generate the source:**
   ```bash
   python3 scripts/merge_python_source.py github
   ```

2. **Update your pipeline:**
   ```python
   source_name = "github"
   
   pipeline_spec = {
       "connection_name": "github_connection",
       "objects": [
           {
               "table": {
                   "source_table": "issues",
                   "table_configuration": {
                       "owner": "apache",
                       "repo": "spark",
                       "state": "all",
                       "start_date": "2024-01-01T00:00:00Z"
                   },
               }
           },
       ],
   }
   ```

3. **Create the appropriate UC connection** with required credentials

See connector-specific READMEs in `sources/{connector}/README.md` for details.

## Troubleshooting

### ImportError: cannot import name 'test_suite' from 'tests'

**Fix:** Make sure `__init__.py` files exist in all package directories. See PR #15.

### ModuleNotFoundError: No module named 'pydantic'

**Fix:** Install dependencies:
```bash
pip install -r requirements.txt
# or activate the virtual environment
source .venv/bin/activate
```

### Connection not found

**Fix:** Verify your Unity Catalog connection exists:
```sql
SHOW CONNECTIONS;
DESCRIBE CONNECTION example_connection;
```

### Table-specific options not passed through

**Fix:** Make sure your connection includes `externalOptionsAllowList` with the required option names (especially for GitHub connector).

## References

- **Lakeflow Community Connectors:** `../sources/`
- **Connector Interface:** `../sources/interface/lakeflow_connect.py`
- **Ingestion Pipeline:** `../pipeline/ingestion_pipeline.py`
- **Source Loader:** `../libs/source_loader.py`
- **Merge Script:** `../scripts/merge_python_source.py`

