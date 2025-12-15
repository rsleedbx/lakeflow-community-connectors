# Lakeflow Nullsource Community Connector

This documentation provides setup instructions and reference information for the Nullsource source connector.

The Nullsource connector generates synthetic data with sequential integer primary keys. Each invocation ingests 1,000 rows by default (configurable via `num_rows`), with primary keys incrementing from the last offset. This makes it ideal for testing incremental ingestion patterns and validating pipeline infrastructure.

## Prerequisites

The Nullsource connector is a minimal test connector that requires no external dependencies or credentials. It is designed for:
- Testing the Lakeflow Community Connector framework
- Serving as a simple reference implementation
- Validating pipeline infrastructure

## Setup

### Required Connection Parameters

No connection parameters are required for the Nullsource connector.

| Parameter | Type | Required | Description | Example |
|-----------|------|----------|-------------|---------|
| N/A | N/A | N/A | No parameters needed | N/A |

### Table-Specific Options

The Nullsource connector supports the following table-specific options:

| Option | Type | Required | Description | Example |
|--------|------|----------|-------------|---------|
| `num_rows` | Integer | Yes | Number of rows to generate per read | `1000` |

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
# Note: While nullsource doesn't require authentication parameters (no tokens, passwords, etc.),
# the connection object is still required by the framework, and externalOptionsAllowList must be
# included to allow table-specific options like "num_rows" to be passed through from the pipeline.
databricks connections create \
  --json '{
    "name": "nullsource_connection",
    "connection_type": "GENERIC_LAKEFLOW_CONNECT",
    "options": {
      "sourceName": "nullsource",
      "externalOptionsAllowList": "num_rows"
    },
    "owner": "'$USER_NAME'"
  }'
```

## Supported Objects

The Nullsource connector supports a single table:

### intpk

A minimal table containing a single integer primary key column.

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

1. Update the `pipeline_spec` in the main pipeline file (e.g., `ingest.py`).

Configure the connector with the required `num_rows` option in the `table_configuration`:

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

# Generate the merged source file
python scripts/merge_python_source.py $SOURCE_NAME

# Create temp directory with only required files
copydir() {
trap 'trap - ERR; kill -INT $$' ERR
set -e # exit on error 
TEMP_DIR=$(mktemp -d)
mkdir -p "$TEMP_DIR/libs" "$TEMP_DIR/sources/$SOURCE_NAME" "$TEMP_DIR/pipeline"
cp -v libs/source_loader.py libs/spec_parser.py libs/utils.py "$TEMP_DIR/libs/"
cp -v sources/$SOURCE_NAME/__init__.py sources/$SOURCE_NAME/_generated_${SOURCE_NAME}_python_source.py "$TEMP_DIR/sources/$SOURCE_NAME/"
cp -v sources/$SOURCE_NAME/ingest.py "$TEMP_DIR/ingest.py"
cp -v pipeline/ingestion_pipeline.py "$TEMP_DIR/pipeline/"

# Sync to Databricks workspace
databricks sync "$TEMP_DIR" "$PROJECT_PATH"

# Cleanup
rm -rf "$TEMP_DIR"
unset "$TEMP_DIR"
echo "Files uploaded to: $WORKSPACE_URL$PROJECT_PATH"
set +e
}

```

##### Create and Run DLT Pipeline

```bash
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
if ! databricks schemas get "$PIPELINE_NAME" main &>/dev/null; then
  echo "Creating schema: main.$PIPELINE_NAME"
  databricks schemas create "$PIPELINE_NAME" main
fi

# Create new pipeline and save ID
databricks pipelines create \
  --json '{
    "name": "'$PIPELINE_NAME'",
    "catalog": "main",
    "schema": "'$PIPELINE_NAME'",
    "configuration": {
      "source_name": "'$SOURCE_NAME'"
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
