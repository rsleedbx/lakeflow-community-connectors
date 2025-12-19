#!/bin/bash
# Simple script to copy entire directory from Databricks workspace
# Uses databricks workspace export-dir for efficiency
# Usage: ./copy_from_workspace_simple.sh [source_path] [destination_path]

set -e

# Default source path
DEFAULT_SOURCE="/Workspace/Users/robert.lee@databricks.com/community_connectors/hubspot/robert_lee_hubspot"

# Parse arguments
SOURCE_PATH="${1:-$DEFAULT_SOURCE}"
DEST_PATH="${2:-./downloaded_workspace}"

echo "================================================================================"
echo "📥 Copying from Databricks Workspace (Simple Method)"
echo "================================================================================"
echo ""
echo "Source (Workspace): $SOURCE_PATH"
echo "Destination (Local): $DEST_PATH"
echo ""

# Create destination directory
mkdir -p "$DEST_PATH"

echo "🚀 Starting download..."
echo ""

# Use export-dir for recursive export
databricks workspace export-dir "$SOURCE_PATH" "$DEST_PATH" --overwrite

echo ""
echo "================================================================================"
echo "✅ Download Complete!"
echo "================================================================================"
echo ""
echo "📂 Files downloaded to: $DEST_PATH"
echo ""
echo "📊 Summary:"
echo "   Total size: $(du -sh "$DEST_PATH" | cut -f1)"
echo "   Total files: $(find "$DEST_PATH" -type f | wc -l | xargs)"
echo "   Total directories: $(find "$DEST_PATH" -type d | wc -l | xargs)"
echo ""
echo "📝 To explore:"
echo "   cd $DEST_PATH"
echo "   tree"
echo ""


