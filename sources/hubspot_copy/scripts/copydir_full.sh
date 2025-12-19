#!/bin/bash
# Deploy ENTIRE repository to Databricks workspace (matching UI upload behavior)
# Usage: ./copydir_full.sh

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

echo "Deploying $SOURCE_NAME connector (FULL REPO)..."
echo "User: $USER_NAME"
echo "Destination: $PROJECT_PATH"
echo ""

# Generate the merged source file
echo "Generating merged source file..."
python3 scripts/merge_python_source.py $SOURCE_NAME

echo ""
echo "Syncing ENTIRE repository to Databricks workspace..."
echo "This will upload all files (like the UI does)"
echo ""

# Use databricks sync on the entire repo root
databricks sync "$REPO_ROOT" "$PROJECT_PATH"

echo ""
echo "✅ Deployment complete!"
echo ""
echo "📁 Workspace location:"
echo "   $PROJECT_PATH"
echo ""
echo "🔗 View files in browser:"
echo "   $WORKSPACE_URL/#workspace$PROJECT_PATH"
echo ""
echo "📊 Uploaded structure matches hubspot_ui:"
echo "   • All repo root files (README.md, pyproject.toml, etc.)"
echo "   • libs/ (with tests)"
echo "   • pipeline/ (with tests)"  
echo "   • sources/$SOURCE_NAME/"
echo "   • ingest.py"
echo ""

set +e


