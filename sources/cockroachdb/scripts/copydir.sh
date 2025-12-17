#!/bin/bash
# Deploy CockroachDB connector to Databricks workspace
# Usage: ./copydir.sh

# Exit on error
trap 'trap - ERR; kill -INT $$' ERR
set -e

SOURCE_NAME="cockroachdb"

# Get current username and workspace URL
USER_NAME=$(databricks current-user me --output json | jq -r '.userName')
WORKSPACE_URL=$(databricks auth env --output json | jq -r '.env.DATABRICKS_HOST')

# Set paths
WORKSPACE_PATH="/Workspace/Users/$USER_NAME"
PROJECT_NAME="${SOURCE_NAME}"
PROJECT_PATH="$WORKSPACE_PATH/$PROJECT_NAME"

echo "Deploying $SOURCE_NAME connector..."
echo "User: $USER_NAME"
echo "Destination: $PROJECT_PATH"
echo ""

# Generate the merged source file
echo "Generating merged source file..."
python3 scripts/merge_python_source.py $SOURCE_NAME

# Create temp directory with only required files
TEMP_DIR=$(mktemp -d)
echo "Created temp directory: $TEMP_DIR"

# Create directory structure
mkdir -p "$TEMP_DIR/libs" "$TEMP_DIR/sources/$SOURCE_NAME" "$TEMP_DIR/pipeline"

# Copy required files
echo "Copying files..."
cp -v libs/source_loader.py libs/spec_parser.py libs/utils.py "$TEMP_DIR/libs/"
cp -v sources/$SOURCE_NAME/__init__.py sources/$SOURCE_NAME/_generated_${SOURCE_NAME}_python_source.py "$TEMP_DIR/sources/$SOURCE_NAME/"
cp -v pipeline/ingestion_pipeline.py "$TEMP_DIR/pipeline/"

# Copy ingest.py and the original connector file (required for list_tables() call)
cp -v sources/$SOURCE_NAME/ingest.py "$TEMP_DIR/ingest.py"
cp -v sources/$SOURCE_NAME/${SOURCE_NAME}.py "$TEMP_DIR/sources/$SOURCE_NAME/"

# Sync to Databricks workspace
echo ""
echo "Syncing to Databricks workspace..."
databricks sync "$TEMP_DIR" "$PROJECT_PATH"

# Cleanup
echo ""
echo "Cleaning up temp directory..."
rm -rf "$TEMP_DIR"
unset "$TEMP_DIR"

echo ""
echo "✅ Deployment complete!"
echo "Files uploaded to: $WORKSPACE_URL$PROJECT_PATH"

set +e

