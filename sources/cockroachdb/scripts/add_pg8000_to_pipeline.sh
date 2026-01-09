#!/usr/bin/env bash
set -e

# Add pg8000 library to existing pipeline
# This script updates the pipeline configuration to include pg8000 as a PyPI dependency

source .env

PIPELINE_ID=$(databricks pipelines get "$PIPELINE_NAME" --output json 2>/dev/null | jq -r '.pipeline_id' || echo "")

if [ -z "$PIPELINE_ID" ]; then
    echo "❌ Pipeline '$PIPELINE_NAME' not found"
    echo "💡 Run createpipeline.sh to create a new pipeline with pg8000 included"
    exit 1
fi

echo "📋 Found pipeline: $PIPELINE_NAME (ID: $PIPELINE_ID)"
echo "📦 Adding pg8000>=1.30.0 to pipeline libraries..."

# Get current pipeline spec
CURRENT_SPEC=$(databricks pipelines get "$PIPELINE_ID" --output json)

# Extract ingest path from current spec
INGEST_PATH=$(echo "$CURRENT_SPEC" | jq -r '.spec.libraries[0].file.path')

# Create updated libraries array with both file and pypi libraries
UPDATED_LIBRARIES=$(cat <<JSON
[
  {
    "file": {
      "path": "$INGEST_PATH"
    }
  },
  {
    "pypi": {
      "package": "pg8000>=1.30.0"
    }
  }
]
JSON
)

# Update the pipeline
databricks pipelines update "$PIPELINE_ID" --libraries="$UPDATED_LIBRARIES"

echo "✅ Pipeline updated successfully!"
echo "📚 Libraries now include:"
echo "   - File: $INGEST_PATH"
echo "   - PyPI: pg8000>=1.30.0"
echo ""
echo "🔄 Restart your pipeline to pick up the new library:"
echo "   databricks pipelines start-update $PIPELINE_ID"
