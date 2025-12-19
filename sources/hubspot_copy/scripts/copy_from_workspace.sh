#!/bin/bash
# Copy all files and subdirectories from a Databricks workspace path
# Usage: ./copy_from_workspace.sh [source_path] [destination_path]

set -e

# Default source path
DEFAULT_SOURCE="/Workspace/Users/robert.lee@databricks.com/community_connectors/hubspot/robert_lee_hubspot"

# Parse arguments
SOURCE_PATH="${1:-$DEFAULT_SOURCE}"
DEST_PATH="${2:-./downloaded_workspace}"

echo "=" * 80
echo "📥 Copying from Databricks Workspace"
echo "=" * 80
echo ""
echo "Source (Workspace): $SOURCE_PATH"
echo "Destination (Local): $DEST_PATH"
echo ""

# Create destination directory if it doesn't exist
mkdir -p "$DEST_PATH"

echo "🔍 Listing workspace contents..."
echo ""

# Function to recursively list and download files
download_recursive() {
    local workspace_path="$1"
    local local_path="$2"
    
    echo "📂 Processing: $workspace_path"
    
    # List items in this directory
    items=$(databricks workspace list "$workspace_path" --output json 2>/dev/null || echo "[]")
    
    # Check if directory is empty or doesn't exist
    if [ "$items" = "[]" ]; then
        echo "   ⚠️  Empty or inaccessible: $workspace_path"
        return
    fi
    
    # Process each item
    echo "$items" | jq -r '.[] | "\(.object_type)|\(.path)"' | while IFS='|' read -r obj_type obj_path; do
        # Get the relative name
        item_name=$(basename "$obj_path")
        local_item_path="$local_path/$item_name"
        
        if [ "$obj_type" = "DIRECTORY" ]; then
            # Create local directory and recurse
            mkdir -p "$local_item_path"
            echo "   📁 Directory: $item_name"
            download_recursive "$obj_path" "$local_item_path"
        elif [ "$obj_type" = "NOTEBOOK" ]; then
            # Export notebook as source file
            echo "   📓 Notebook: $item_name"
            databricks workspace export "$obj_path" "$local_item_path" --format SOURCE 2>/dev/null || \
            databricks workspace export "$obj_path" "$local_item_path.py" --format SOURCE 2>/dev/null || \
            echo "      ⚠️  Failed to export notebook: $item_name"
        elif [ "$obj_type" = "FILE" ]; then
            # Export regular file
            echo "   📄 File: $item_name"
            databricks workspace export "$obj_path" "$local_item_path" --format AUTO 2>/dev/null || \
            echo "      ⚠️  Failed to export file: $item_name"
        else
            echo "   ❓ Unknown type ($obj_type): $item_name"
        fi
    done
}

# Start recursive download
download_recursive "$SOURCE_PATH" "$DEST_PATH"

echo ""
echo "=" * 80
echo "✅ Download Complete!"
echo "=" * 80
echo ""
echo "Files downloaded to: $DEST_PATH"
echo ""
echo "📊 Summary:"
du -sh "$DEST_PATH"
find "$DEST_PATH" -type f | wc -l | xargs echo "Total files:"
find "$DEST_PATH" -type d | wc -l | xargs echo "Total directories:"
echo ""


