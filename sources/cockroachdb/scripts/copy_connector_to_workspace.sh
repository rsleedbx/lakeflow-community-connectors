#!/usr/bin/env bash
# Copy CockroachDB connector code to Databricks workspace
#
# This script copies connector files to the workspace but does NOT create the pipeline.
# Pipeline creation is done separately by deploy_pipeline.sh or databricks bundle.
#
# Usage: ./copy_connector_to_workspace.sh [MODE_SUFFIX]
#    MODE_SUFFIX: Optional suffix like "_direct", "_volume", etc.
#
# Examples:
#   ./copy_connector_to_workspace.sh           # No suffix: cockroachdb/
#   ./copy_connector_to_workspace.sh _direct   # With suffix: cockroachdb_direct/

# Exit on error
trap 'trap - ERR; kill -INT $$' ERR
set -e

SOURCE_NAME="cockroachdb"
MODE_SUFFIX="${1:-}"  # Optional mode suffix parameter

# Find repository root using git
REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null)"
if [ -z "$REPO_ROOT" ]; then
  echo "❌ Error: Not in a git repository"
  echo "   This script must be run from within the lakeflow-community-connectors repository"
  exit 1
fi

# Verify we found the repo root
if [ ! -f "$REPO_ROOT/scripts/merge_python_source.py" ]; then
  echo "❌ Error: Could not find required script"
  echo "   Looking for: $REPO_ROOT/scripts/merge_python_source.py"
  exit 1
fi

# Change to repository root
cd "$REPO_ROOT"
echo "Repository root: $REPO_ROOT"
echo ""

# Get current username and workspace URL
USER_NAME=$(databricks current-user me --output json | jq -r '.userName')
WORKSPACE_URL=$(databricks auth env --output json | jq -r '.env.DATABRICKS_HOST')

# Set paths (append mode suffix if provided)
WORKSPACE_PATH="/Workspace/Users/$USER_NAME"
PROJECT_NAME="${SOURCE_NAME}${MODE_SUFFIX}"
PROJECT_PATH="$WORKSPACE_PATH/$PROJECT_NAME"

echo "Copying $SOURCE_NAME connector code to workspace..."
echo "User: $USER_NAME"
echo "Destination: $PROJECT_PATH"
if [ -n "$MODE_SUFFIX" ]; then
    echo "Mode suffix: $MODE_SUFFIX"
fi
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

# Note: pg8000 is now installed via pipeline libraries (requirements.txt)
# No need to vendor or copy dependencies manually

# Sync to Databricks workspace
echo ""
echo "Syncing to Databricks workspace..."
databricks sync "$TEMP_DIR" "$PROJECT_PATH"

# Cleanup
echo ""
echo "Cleaning up temp directory..."
rm -rf "$TEMP_DIR"
unset TEMP_DIR

echo ""
echo "✅ Connector code copied to workspace!"
echo ""
echo "📁 Workspace location:"
echo "   $PROJECT_PATH"
echo ""
echo "🔗 View files in browser:"
echo "   $WORKSPACE_URL/#workspace$PROJECT_PATH"
echo ""

set +e

