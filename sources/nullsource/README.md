# Lakeflow Nullsource Community Connector

This documentation provides setup instructions and reference information for the Nullsource source connector.

The Nullsource connector generates synthetic data with sequential integer primary keys. Each invocation ingests 1,000 rows by default (configurable via `num_rows` in pipeline configuration or per-table configuration), with primary keys incrementing from the last offset. This makes it ideal for testing incremental ingestion patterns and validating pipeline infrastructure.

## Prerequisites

The Nullsource connector is a minimal test connector that requires no external dependencies or credentials. It is designed for:
- Testing the Lakeflow Community Connector framework
- Serving as a simple reference implementation
- Validating pipeline infrastructure

## Setup

### Required Connection Parameters

No connection parameters are required for the Nullsource connector. The connection only needs to specify `sourceName: "nullsource"`.

| Parameter | Type | Required | Description | Example |
|-----------|------|----------|-------------|---------|
| N/A | N/A | N/A | No connection parameters needed | N/A |

### Pipeline Configuration Parameters

For multi-table pipelines, the following parameters can be set in the DLT pipeline configuration:

| Parameter | Type | Required | Description | Example |
|-----------|------|----------|-------------|---------|
| `num_tables` | Integer | No | Number of tables to dynamically generate (default: 1) | `"3"` |
| `num_rows` | Integer | No | Default number of rows per table if not specified per table (default: 1000) | `"5000"` |

These parameters are:
- Set in the DLT pipeline `configuration` section (as shown in the pipeline creation JSON below)
- Exposed by DLT as **Spark configuration** values (accessed via `spark.conf.get()`)
- `num_tables` is read by `ingest.py` at runtime to discover all available tables
- `num_rows` is used as the default for all tables unless overridden in per-table configuration

### Table-Specific Options

The Nullsource connector supports the following table-specific options:

| Option | Type | Required | Description | Example |
|--------|------|----------|-------------|---------|
| `num_rows` | Integer | No | Number of rows to generate per read (overrides pipeline default) | `1000` |

**Configuration Priority**: If `num_rows` is specified in table-specific configuration, it overrides the pipeline-level `num_rows` default. If neither is specified, the connector defaults to 1000 rows.

Since the connector supports table-specific options, you must include `externalOptionsAllowList` in the connection configuration:

```json
{
  "externalOptionsAllowList": "num_rows"
}
```

### Development Workflow

The deployment instructions in this README are optimized for **laptop-based AI-assisted development** with fast iteration cycles. 

Traditional workflows (git commit → push → workspace pull → test) are too slow for AI-assisted development where you're making frequent changes and need immediate feedback. Instead, this approach uses direct file uploads via `databricks sync` to push changes from your local development environment directly to the Databricks workspace for testing.

**Benefits of this approach:**
- ⚡ **Fast iteration**: Changes are live in seconds, not minutes
- 🤖 **AI-friendly**: Works seamlessly with AI coding assistants making rapid changes
- 🔄 **Immediate feedback**: Test your changes immediately without git operations
- 💻 **Local-first**: Develop and test on your laptop, commit to git only when ready

Once you've validated your connector works in Databricks, you can commit the final version to git for production use.

### Create a Unity Catalog Connection 

A Unity Catalog connection for this connector can be created in two ways via the UI:
1. Follow the Lakeflow Community Connector UI flow from the "Add Data" page
2. Select any existing Lakeflow Community Connector connection for this source or create a new one.

The connection can also be created using the standard Unity Catalog API or Databricks CLI.

**Note**: Although the Nullsource connector does not use any connection parameters for authentication or configuration, a Unity Catalog connection is still **required** by the Lakeflow framework. The pipeline references this connection name in its configuration, even though the connector itself doesn't read any credentials from it.

#### Creating Connection via Databricks CLI

```bash
# Get current username
USER_NAME=$(databricks current-user me --output json | jq -r '.userName')

# Create the connection
# Note: This single connection is used for both single-table and multi-table pipelines.
# For multi-table mode, num_tables is passed via Spark configuration.
databricks connections create \
  --json '{
    "name": "nullsource_connection",
    "connection_type": "GENERIC_LAKEFLOW_CONNECT",
    "options": {
      "sourceName": "nullsource",
      "externalOptionsAllowList": "num_rows"
    }
  }'
```

## Supported Objects

The Nullsource connector dynamically generates tables based on the `num_tables` parameter (default: 1). For single-table pipelines, this defaults to 1. For multi-table pipelines, set `num_tables` in the DLT pipeline configuration.

### intpk (and intpk_00002, intpk_00003, ... if num_tables > 1)

Minimal tables containing a single integer primary key column. The first table is always named `intpk`, and additional tables are named `intpk_00002`, `intpk_00003`, etc. (5-digit zero-padded). Each table is independent with its own offset tracking.

- **Primary Key**: `pk` (Long)
- **Ingestion Type**: `append`
- **Incremental Strategy**: Offset-based tracking (remembers how many rows were generated)
- **Schema**:
  - `pk`: Long (not nullable) - Primary key field that increments from 0
- **Configuration**:
  - `num_rows` (required): Number of rows to generate per read operation

This table generates sequential rows starting from `pk = 0` and tracks progress via offset. Each read generates `num_rows` rows with incrementing primary keys.

## Data Type Mapping

| Source Type | Databricks Type | Notes |
|-------------|-----------------|-------|
| Integer | Long | Primary key field |

## How to Run

### Step 1: Clone/Copy the Source Connector Code

Follow the Lakeflow Community Connector UI, which will guide you through setting up a pipeline using the selected source connector code.

Alternatively, you can manually clone the repository:

```bash
git clone https://github.com/yyoli-db/lakeflow-community-connectors.git
cd lakeflow-community-connectors
```

### Step 2: Configure Your Pipeline

The `ingest.py` file automatically discovers tables based on the `num_tables` pipeline configuration and uses `num_rows` as the default for all tables. You can customize this behavior in several ways:

**Option 1: Use pipeline-level defaults** (simplest, recommended)

Set `num_tables` and `num_rows` in the DLT pipeline configuration (see pipeline creation below). All tables will use these defaults.

**Option 2: Manually configure specific tables**

Update the `pipeline_spec` in `ingest.py` to explicitly configure each table:

```json
{
  "connection_name": "nullsource_connection",
  "objects": [
    {
      "table": {
        "source_table": "intpk",
        "table_configuration": {
          "num_rows": "1000"
        }
      }
    }
  ]
}
```

The `num_rows` option controls how many rows are generated per read operation. For testing, you can start with a smaller value like `"100"` and increase as needed. Note that values in `table_configuration` must be strings.

**Example with multiple tables** (if you created the connection with `num_tables: "3"`):

```json
{
  "connection_name": "nullsource_connection",
  "objects": [
    {
      "table": {
        "source_table": "intpk",
        "table_configuration": {
          "num_rows": "500"
        }
      }
    },
    {
      "table": {
        "source_table": "intpk_00002",
        "table_configuration": {
          "num_rows": "1000"
        }
      }
    },
    {
      "table": {
        "source_table": "intpk_00003",
        "table_configuration": {
          "num_rows": "2000"
        }
      }
    }
  ]
}
```

**Option 3: How the default dynamic discovery works**

The provided `ingest.py` automatically discovers and configures all tables using this approach:

```python
from pipeline.ingestion_pipeline import ingest
from libs.source_loader import get_register_function
from sources.nullsource.nullsource import LakeflowConnect

source_name = "nullsource"
connection_name = "nullsource_connection"

# Read configuration from DLT pipeline configuration (via Spark config)
num_tables_str = spark.conf.get("num_tables", "1")
num_rows_default = spark.conf.get("num_rows", "1000")

# Create connector instance to discover available tables
connector = LakeflowConnect({"num_tables": num_tables_str})
all_tables = connector.list_tables()  # Returns ["intpk", "intpk_00002", "intpk_00003"]

# Generate pipeline spec for all discovered tables using default num_rows
default_table_config = {"num_rows": num_rows_default}

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

# Register and run pipeline
register_lakeflow_source = get_register_function(source_name)
register_lakeflow_source(spark)
ingest(spark, pipeline_spec)
```

This approach automatically:
- Reads `num_tables` and `num_rows` from the DLT pipeline configuration (set during pipeline creation)
- Discovers all available tables via `list_tables()`
- Generates the pipeline spec to ingest all tables with the default `num_rows` configuration

**Overriding `num_rows` for specific tables**:

To use different `num_rows` values for specific tables while keeping the default for others, modify the pipeline spec generation:

```python
# Use pipeline default for most tables, but override for specific ones
table_configs = {
    "intpk": {"num_rows": "5000"},      # Override: 5000 rows
    "intpk_00002": {"num_rows": "10000"}  # Override: 10000 rows
    # All other tables will use the pipeline default (num_rows_default)
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

**Note**: This approach requires:
- The original connector file (`nullsource.py`) to be uploaded to the workspace (for the `list_tables()` call)
- The `num_tables` value to be set in the DLT pipeline configuration (see pipeline creation below)

The implementation in `sources/nullsource/ingest.py` handles this automatically. When deploying, ensure both the connector file (`nullsource.py`) and `ingest.py` are uploaded (see deployment instructions below).

2. (Optional) Customize the source connector code if needed for special use cases.

### Step 3: Run and Schedule the Pipeline

#### Deployment to Databricks

##### Upload Files to Databricks Workspace

```bash
# Set source name
SOURCE_NAME="nullsource"

# Get current username and workspace URL
USER_NAME=$(databricks current-user me --output json | jq -r '.userName')
WORKSPACE_URL=$(databricks auth env --output json | jq -r '.env.DATABRICKS_HOST')

# Set paths
WORKSPACE_PATH="/Workspace/Users/$USER_NAME"
PROJECT_NAME="${SOURCE_NAME}"
PROJECT_PATH="$WORKSPACE_PATH/$PROJECT_NAME"


# Create temp directory with only required files
copydir() {
trap 'trap - ERR; kill -INT $$' ERR
set -e # exit on error 
# Generate the merged source file
python3 scripts/merge_python_source.py $SOURCE_NAME

TEMP_DIR=$(mktemp -d)
mkdir -p "$TEMP_DIR/libs" "$TEMP_DIR/sources/$SOURCE_NAME" "$TEMP_DIR/pipeline"
cp -v libs/source_loader.py libs/spec_parser.py libs/utils.py "$TEMP_DIR/libs/"
cp -v sources/$SOURCE_NAME/__init__.py sources/$SOURCE_NAME/_generated_${SOURCE_NAME}_python_source.py "$TEMP_DIR/sources/$SOURCE_NAME/"
cp -v pipeline/ingestion_pipeline.py "$TEMP_DIR/pipeline/"

# Copy ingest.py and the original connector file (required for list_tables() call)
cp -v sources/$SOURCE_NAME/ingest.py "$TEMP_DIR/ingest.py"
cp -v sources/$SOURCE_NAME/${SOURCE_NAME}.py "$TEMP_DIR/sources/$SOURCE_NAME/"

# Sync to Databricks workspace
databricks sync "$TEMP_DIR" "$PROJECT_PATH"

# Cleanup
rm -rf "$TEMP_DIR"
unset "$TEMP_DIR"
echo "Files uploaded to: $WORKSPACE_URL$PROJECT_PATH"
set +e
}

```

**Deploy the connector:**
```bash
copydir  # Uploads all required files including nullsource.py and ingest.py
```

##### Create and Run DLT Pipeline

```bash
createpipeline() {
local NUM_TABLES=${1:-1}  # Default to 1 if not specified
local NUM_ROWS=${2:-1000}  # Default to 1000 if not specified

# Create pipeline (deletes existing if name conflicts)
PIPELINE_NAME="$(echo $USER_NAME | cut -d'@' -f1 | tr '.' '_')_${SOURCE_NAME}"

# Check if pipeline exists and delete it
PIPELINE_ID=$(databricks pipelines list-pipelines \
  --filter "name LIKE '$PIPELINE_NAME'" \
  --output json | jq -r '.[0].pipeline_id // empty')

if [ -n "$PIPELINE_ID" ]; then
  echo "Deleting existing pipeline: $PIPELINE_ID"
  databricks pipelines delete "$PIPELINE_ID"
  sleep 5
fi

# Create schema if it doesn't exist
if ! databricks schemas get "main.${PIPELINE_NAME}" &>/dev/null; then
  echo "Creating schema: main.${PIPELINE_NAME}"
  databricks schemas create "${PIPELINE_NAME}" main
fi

# Create new pipeline and save ID
databricks pipelines create \
  --json '{
    "name": "'$PIPELINE_NAME'",
    "catalog": "main",
    "schema": "'${PIPELINE_NAME}'",
    "configuration": {
      "source_name": "'$SOURCE_NAME'",
      "num_tables": "'${NUM_TABLES}'",
      "num_rows": "'${NUM_ROWS}'"
    },
    "serverless": true,
    "continuous": false,
    "development": true,
    "libraries": [
      {
        "file": {
          "path": "'$PROJECT_PATH'/ingest.py"
        }
      }
    ]
  }' | tee /tmp/$PIPELINE_NAME.$$

# Extract pipeline ID from saved response
PIPELINE_ID=$(cat /tmp/$PIPELINE_NAME.$$ | jq -r '.pipeline_id')

echo ""
echo "Pipeline created with ID: $PIPELINE_ID"
echo "View pipeline at: $WORKSPACE_URL/pipelines/$PIPELINE_ID"
}

# Create single-table pipeline (1 table, 1000 rows)
createpipeline

# OR create multi-table pipeline with 2 tables, 1000 rows each
createpipeline 2

# OR create multi-table pipeline with 3 tables, 5000 rows each
createpipeline 3 5000

# OR create multi-table pipeline with 2 tables, 10000 rows each
createpipeline 2 10000
```

##### Start the Pipeline

Once the pipeline is created, start it to begin ingestion:

```bash
# Start the pipeline
databricks pipelines start-update "$PIPELINE_ID"

echo "Pipeline started. Monitor progress at: $WORKSPACE_URL/pipelines/$PIPELINE_ID"
```

The pipeline will run and ingest data from the nullsource connector. You can monitor the progress in the Databricks UI.

To check the pipeline status:

```bash
# Get pipeline status
databricks pipelines get "$PIPELINE_ID" --output json | jq -r '.state'
```

##### Update Pipeline Configuration

To update `num_tables` or `num_rows` on an existing pipeline without recreating it:

```bash
# Get current pipeline spec
CURRENT_SPEC=$(databricks pipelines get "$PIPELINE_ID" --output json | jq '.spec')

# Update configuration options while preserving all other settings
UPDATED_SPEC=$(echo "$CURRENT_SPEC" | jq '.configuration.num_tables = "2" | .configuration.num_rows = "1"')

# Apply the update
databricks pipelines update "$PIPELINE_ID" --json "$UPDATED_SPEC"

# Verify the update
databricks pipelines get "$PIPELINE_ID" --output json | jq -r '.spec.configuration'

# Start a fresh update to apply changes
databricks pipelines start-update "$PIPELINE_ID" --full-refresh
```

**Note**: This approach fetches the existing pipeline spec and only modifies the configuration fields, preserving all other settings (name, catalog, schema, libraries, serverless mode, etc.). Configuration changes only take effect when the pipeline runs. Use `--full-refresh` to ensure all tables are reprocessed with the new configuration.

**Examples**:

```bash
# Increase rows per table from 1000 to 5000
CURRENT_SPEC=$(databricks pipelines get "$PIPELINE_ID" --output json | jq '.spec')
UPDATED_SPEC=$(echo "$CURRENT_SPEC" | jq '.configuration.num_rows = "5000"')
databricks pipelines update "$PIPELINE_ID" --json "$UPDATED_SPEC"

# Add more tables (from 2 to 10)
CURRENT_SPEC=$(databricks pipelines get "$PIPELINE_ID" --output json | jq '.spec')
UPDATED_SPEC=$(echo "$CURRENT_SPEC" | jq '.configuration.num_tables = "10"')
databricks pipelines update "$PIPELINE_ID" --json "$UPDATED_SPEC"

# Update both num_tables and num_rows
CURRENT_SPEC=$(databricks pipelines get "$PIPELINE_ID" --output json | jq '.spec')
UPDATED_SPEC=$(echo "$CURRENT_SPEC" | jq '.configuration.num_tables = "3" | .configuration.num_rows = "5000"')
databricks pipelines update "$PIPELINE_ID" --json "$UPDATED_SPEC"

# Scale down for testing (fewer rows)
CURRENT_SPEC=$(databricks pipelines get "$PIPELINE_ID" --output json | jq '.spec')
UPDATED_SPEC=$(echo "$CURRENT_SPEC" | jq '.configuration.num_rows = "100"')
databricks pipelines update "$PIPELINE_ID" --json "$UPDATED_SPEC"
```

#### Best Practices

- **Start Small**: The Nullsource connector only has one table, making it ideal for testing
- **Use for Testing**: This connector is designed for validating pipeline infrastructure, not production use
- **No Rate Limits**: Since this is a synthetic source, there are no API rate limits to consider

#### Troubleshooting

**Common Issues:**

1. **Import Errors**: Ensure all required files are uploaded to the workspace
   - Check that `libs/source_loader.py`, `pipeline/ingestion_pipeline.py`, and the generated source file are present

2. **Connection Not Found**: Verify the connection name in your pipeline spec matches the created Unity Catalog connection
   - Use `databricks connections list` to check available connections

3. **Schema Mismatch**: The `intpk` table schema is fixed with a single `pk` column of type Long
   - Ensure your pipeline spec references the correct table name

## References

- [Lakeflow Community Connectors GitHub Repository](https://github.com/yyoli-db/lakeflow-community-connectors)
- [Databricks Unity Catalog Connections Documentation](https://docs.databricks.com/en/connect/unity-catalog/connections.html)
- [Databricks Delta Live Tables Documentation](https://docs.databricks.com/en/delta-live-tables/index.html)
