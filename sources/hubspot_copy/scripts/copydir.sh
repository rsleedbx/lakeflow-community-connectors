#!/bin/bash
# Deploy hubspot_copy connector to Databricks workspace
# Matches the exact structure from hubspot_ui workspace
# Usage: ./copydir.sh

# Exit on error
trap 'trap - ERR; kill -INT $$' ERR
set -e

SOURCE_NAME="hubspot_copy"

# Find repository root using git
REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null)"
if [ -z "$REPO_ROOT" ]; then
  echo "❌ Error: Not in a git repository"
  echo "   This script must be run from within the lakeflow-community-connectors repository"
  exit 1
fi

# Change to repository root
cd "$REPO_ROOT"
echo "Repository root: $REPO_ROOT"
echo ""

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

# Create temp directory matching hubspot_ui structure
TEMP_DIR=$(mktemp -d)
echo "Created temp directory: $TEMP_DIR"
echo ""

# Create directory structure
echo "Creating directory structure..."
mkdir -p "$TEMP_DIR/libs/test"
mkdir -p "$TEMP_DIR/pipeline/test"
mkdir -p "$TEMP_DIR/sources/$SOURCE_NAME/test"

# Copy root files (matching hubspot_ui)
echo "Copying root files..."
[ -f "README.md" ] && cp -v "README.md" "$TEMP_DIR/"
[ -f "pyproject.toml" ] && cp -v "pyproject.toml" "$TEMP_DIR/"
[ -f "CONTRIBUTING.md" ] && cp -v "CONTRIBUTING.md" "$TEMP_DIR/"
[ -f "LICENSE" ] && cp -v "LICENSE" "$TEMP_DIR/"
[ -f "NOTICE" ] && cp -v "NOTICE" "$TEMP_DIR/"

# Copy ingest.py
cp -v "sources/$SOURCE_NAME/ingest.py" "$TEMP_DIR/ingest.py"

# Copy libs (including tests)
echo ""
echo "Copying libs..."
cp -v "libs/source_loader.py" "$TEMP_DIR/libs/"
cp -v "libs/spec_parser.py" "$TEMP_DIR/libs/"
cp -v "libs/utils.py" "$TEMP_DIR/libs/"
if [ -d "libs/test" ]; then
  cp -v "libs/test/test_spec_parser.py" "$TEMP_DIR/libs/test/" 2>/dev/null || true
  cp -v "libs/test/test_utils.py" "$TEMP_DIR/libs/test/" 2>/dev/null || true
fi

# Copy pipeline (including tests)
echo ""
echo "Copying pipeline..."
cp -v "pipeline/ingestion_pipeline.py" "$TEMP_DIR/pipeline/"
cp -v "pipeline/lakeflow_python_source.py" "$TEMP_DIR/pipeline/"
if [ -d "pipeline/test" ]; then
  cp -v "pipeline/test/test_ingestion_pipeline.py" "$TEMP_DIR/pipeline/test/" 2>/dev/null || true
fi

# Copy sources/hubspot_copy (including tests)
echo ""
echo "Copying sources/$SOURCE_NAME..."
cp -v "sources/$SOURCE_NAME/${SOURCE_NAME}.py" "$TEMP_DIR/sources/$SOURCE_NAME/"
cp -v "sources/$SOURCE_NAME/_generated_${SOURCE_NAME}_python_source.py" "$TEMP_DIR/sources/$SOURCE_NAME/"
[ -f "sources/$SOURCE_NAME/README.md" ] && cp -v "sources/$SOURCE_NAME/README.md" "$TEMP_DIR/sources/$SOURCE_NAME/"
[ -f "sources/$SOURCE_NAME/${SOURCE_NAME}_test_utils.py" ] && cp -v "sources/$SOURCE_NAME/${SOURCE_NAME}_test_utils.py" "$TEMP_DIR/sources/$SOURCE_NAME/" || true
if [ -d "sources/$SOURCE_NAME/test" ]; then
  cp -v "sources/$SOURCE_NAME/test/test_${SOURCE_NAME}_lakeflow_connect.py" "$TEMP_DIR/sources/$SOURCE_NAME/test/" 2>/dev/null || true
fi

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
echo "✅ Deployment complete!"
echo ""
echo "📁 Workspace location:"
echo "   $PROJECT_PATH"
echo ""
echo "🔗 View files in browser:"
echo "   $WORKSPACE_URL/#workspace$PROJECT_PATH"
echo ""
echo "📊 Deployed structure (matching hubspot_ui):"
echo "   • Root files: README.md, pyproject.toml, ingest.py"
echo "   • libs/ (with test/)"
echo "   • pipeline/ (with test/)"
echo "   • sources/$SOURCE_NAME/ (with test/)"
echo ""

set +e
