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

**Option 1: Via Databricks CLI (Recommended for automation)**
**Important:** The SDP file requires several dependencies to run. We upload ONLY these files:
- `libs/source_loader.py` - Loads the connector registration function
- `sources/example/_generated_example_python_source.py` - The merged connector code
- `__init__.py` files - Makes Python packages importable

**Note:** We explicitly upload each file to avoid uploading unnecessary connectors (github, hubspot, stripe, zendesk), test files, and documentation.

```bash
# Set user workspace path
USER_PATH="/Workspace/Users/$(databricks current-user me --output json | jq -r '.userName')"

# Upload only required files to example_connector directory
PROJECT_PATH="$USER_PATH/example_connector"

# Create a temporary directory with only the files we need
TEMP_DIR=$(mktemp -d)
mkdir -p "$TEMP_DIR/libs"
mkdir -p "$TEMP_DIR/sources/example"
mkdir -p "$TEMP_DIR/pipeline-spec"

# Copy only required files
cp libs/__init__.py "$TEMP_DIR/libs/"
cp libs/source_loader.py "$TEMP_DIR/libs/"
cp sources/__init__.py "$TEMP_DIR/sources/"
cp sources/example/__init__.py "$TEMP_DIR/sources/example/"
cp sources/example/_generated_example_python_source.py "$TEMP_DIR/sources/example/"
cp pipeline-spec/example_sdp_pipeline.py "$TEMP_DIR/pipeline-spec/"

# Sync the temporary directory (only contains required files)
databricks sync "$TEMP_DIR" "$PROJECT_PATH" --full

# Clean up
rm -rf "$TEMP_DIR"

# Get the URL to view the uploaded project in Databricks UI
WORKSPACE_URL=$(databricks auth env --output json | jq -r '.env.DATABRICKS_HOST')
echo "View uploaded project at: ${WORKSPACE_URL}#workspace${PROJECT_PATH}/"

# Run immediately with jobs submit (using serverless compute)
# Serverless: faster startup, no cluster management, pay-per-use
databricks jobs submit --json "{
  \"run_name\": \"Example SDP Pipeline Run\",
  \"tasks\": [{
    \"task_key\": \"run_pipeline\",
    \"spark_python_task\": {
      \"python_file\": \"$PROJECT_PATH/pipeline-spec/example_sdp_pipeline.py\"
    },
    \"compute\": {
      \"spec\": {
        \"kind\": \"serverless_compute\"
      }
    }
  }]
}"

# Or create a reusable job
databricks jobs create --json "{
  \"name\": \"Example Connector SDP Pipeline\",
  \"tasks\": [{
    \"task_key\": \"run_pipeline\",
    \"spark_python_task\": {
      \"python_file\": \"$PROJECT_PATH/pipeline-spec/example_sdp_pipeline.py\"
    },
    \"compute\": {
      \"spec\": {
        \"kind\": \"serverless_compute\"
      }
    }
  }]
}"
```

**Option 2: Via Notebook**
1. Upload project files to your workspace (using the command above)
2. Create a notebook and run:
   ```python
   %run /Workspace/Users/<your-username>/example_connector/pipeline-spec/example_sdp_pipeline
   ```

**Option 3: Via Jobs UI**
1. Go to **Workflows** → **Jobs** → **Create Job**
2. Add a Python task pointing to your uploaded pipeline file
3. Configure cluster and run

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

