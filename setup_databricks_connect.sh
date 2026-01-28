#!/bin/bash
# Setup Databricks Connect for running pytest locally
# This script helps configure environment variables for Databricks Connect
# Works with VS Code Databricks Extension and Databricks CLI

set -e

echo "🚀 Databricks Connect Setup for pytest"
echo "========================================"
echo ""

# Function to list available profiles
list_profiles() {
    if [ -f ~/.databrickscfg ]; then
        echo "Available profiles:"
        grep '^\[' ~/.databrickscfg | tr -d '[]' | nl -w2 -s'. '
        return 0
    fi
    return 1
}

# Check if .databrickscfg exists
if [ ! -f ~/.databrickscfg ]; then
    echo "⚠️  ~/.databrickscfg not found"
    echo ""
    echo "💡 Tip: If you have VS Code Databricks Extension installed,"
    echo "   you can configure it first, and it will create this file."
    echo ""
    echo "Creating ~/.databrickscfg with template..."
    echo ""
    
    read -p "Enter Databricks workspace URL (e.g., https://your-workspace.cloud.databricks.com): " WORKSPACE_URL
    read -p "Enter personal access token (dapi_xxx): " TOKEN
    read -p "Enter cluster ID (e.g., 1234-567890-abc123): " CLUSTER_ID
    
    cat > ~/.databrickscfg <<EOF
[DEFAULT]
host = ${WORKSPACE_URL}
token = ${TOKEN}
cluster_id = ${CLUSTER_ID}
EOF
    
    echo "✅ Created ~/.databrickscfg"
    echo ""
    PROFILE="DEFAULT"
else
    echo "✅ Found ~/.databrickscfg (used by VS Code Databricks Extension)"
    echo ""
    
    # Check if multiple profiles exist
    PROFILE_COUNT=$(grep -c '^\[' ~/.databrickscfg || echo "0")
    
    if [ "$PROFILE_COUNT" -gt 1 ]; then
        echo "Multiple profiles found:"
        list_profiles
        echo ""
        read -p "Enter profile name to use (or press Enter for DEFAULT): " PROFILE
        PROFILE=${PROFILE:-DEFAULT}
    else
        PROFILE="DEFAULT"
    fi
    
    echo "Using profile: $PROFILE"
    echo ""
fi

# Read configuration from selected profile
if [ "$PROFILE" = "DEFAULT" ]; then
    HOST=$(grep "^host" ~/.databrickscfg | head -1 | cut -d'=' -f2- | tr -d ' ')
    TOKEN=$(grep "^token" ~/.databrickscfg | head -1 | cut -d'=' -f2- | tr -d ' ')
    CLUSTER_ID=$(grep "^cluster_id" ~/.databrickscfg | head -1 | cut -d'=' -f2- | tr -d ' ')
else
    # Extract from specific profile section
    HOST=$(awk "/^\[$PROFILE\]/,/^\[/ {if (/^host/) print}" ~/.databrickscfg | cut -d'=' -f2- | tr -d ' ')
    TOKEN=$(awk "/^\[$PROFILE\]/,/^\[/ {if (/^token/) print}" ~/.databrickscfg | cut -d'=' -f2- | tr -d ' ')
    CLUSTER_ID=$(awk "/^\[$PROFILE\]/,/^\[/ {if (/^cluster_id/) print}" ~/.databrickscfg | cut -d'=' -f2- | tr -d ' ')
fi

# Determine compute type
if [ -z "$CLUSTER_ID" ]; then
    echo "⚠️  cluster_id not found in profile"
    echo ""
    echo "Select compute type:"
    echo "  1) Serverless (recommended - no cluster ID needed)"
    echo "  2) Cluster-based (requires cluster ID)"
    echo ""
    read -p "Enter choice (1 or 2): " COMPUTE_CHOICE
    
    if [ "$COMPUTE_CHOICE" = "2" ]; then
        echo ""
        read -p "Enter cluster ID (e.g., 1234-567890-abc123): " CLUSTER_ID
        
        # Append to profile
        if [ "$PROFILE" = "DEFAULT" ]; then
            echo "cluster_id = $CLUSTER_ID" >> ~/.databrickscfg
        else
            # Insert after profile header
            sed -i.bak "/^\[$PROFILE\]/a\\
cluster_id = $CLUSTER_ID" ~/.databrickscfg
            rm ~/.databrickscfg.bak 2>/dev/null || true
        fi
        
        echo "✅ Added cluster_id to profile"
        echo ""
        USE_SERVERLESS=false
    else
        echo "✅ Using Serverless compute (no cluster ID required)"
        echo ""
        USE_SERVERLESS=true
        CLUSTER_ID=""
    fi
else
    echo "✅ Found cluster_id in profile"
    USE_SERVERLESS=false
fi

# Remove https:// from host for SPARK_REMOTE
HOST_CLEAN=$(echo "$HOST" | sed 's|https://||')

# Build SPARK_REMOTE string
if [ "$USE_SERVERLESS" = "true" ] || [ -z "$CLUSTER_ID" ]; then
    # Serverless - no cluster ID
    SPARK_REMOTE="sc://${HOST_CLEAN}:443/;token=${TOKEN}"
    COMPUTE_TYPE="Serverless"
else
    # Cluster-based - include cluster ID
    SPARK_REMOTE="sc://${HOST_CLEAN}:443/;token=${TOKEN};x-databricks-cluster-id=${CLUSTER_ID}"
    COMPUTE_TYPE="Cluster-based"
fi

echo "📋 Configuration:"
echo "   Host: $HOST"
echo "   Compute Type: $COMPUTE_TYPE"
if [ "$USE_SERVERLESS" != "true" ] && [ -n "$CLUSTER_ID" ]; then
    echo "   Cluster ID: $CLUSTER_ID"
fi
echo "   Token: ${TOKEN:0:15}..."
echo ""

# Export for current shell
export SPARK_REMOTE="$SPARK_REMOTE"

echo "✅ SPARK_REMOTE set for current shell session"
echo ""
echo "To make this permanent, add to your shell profile:"
echo ""

if [[ "$SHELL" == *"zsh"* ]]; then
    PROFILE_FILE="~/.zshrc"
elif [[ "$SHELL" == *"bash"* ]]; then
    PROFILE_FILE="~/.bashrc"
else
    PROFILE_FILE="~/.profile"
fi

echo "echo 'export SPARK_REMOTE=\"${SPARK_REMOTE}\"' >> ${PROFILE_FILE}"
echo ""

# Test connection if databricks-connect is installed
if python -c "import databricks.connect" 2>/dev/null; then
    echo "🔍 Testing Databricks Connect connection..."
    
    # Build Python test command based on compute type
    if [ "$USE_SERVERLESS" = "true" ] || [ -z "$CLUSTER_ID" ]; then
        # Serverless - no cluster_id parameter
        PYTHON_TEST="
from databricks.connect import DatabricksSession
try:
    spark = DatabricksSession.builder.remote(
        host='${HOST}',
        token='${TOKEN}'
    ).getOrCreate()
    print(f'✅ Connected! Spark version: {spark.version}')
    print('✅ Compute type: Serverless')
except Exception as e:
    print(f'❌ Connection failed: {e}')
    exit(1)
"
    else
        # Cluster-based - include cluster_id
        PYTHON_TEST="
from databricks.connect import DatabricksSession
try:
    spark = DatabricksSession.builder.remote(
        host='${HOST}',
        token='${TOKEN}',
        cluster_id='${CLUSTER_ID}'
    ).getOrCreate()
    print(f'✅ Connected! Spark version: {spark.version}')
    print('✅ Compute type: Cluster-based')
except Exception as e:
    print(f'❌ Connection failed: {e}')
    exit(1)
"
    fi
    
    if python -c "$PYTHON_TEST" 2>&1; then
        echo ""
        echo "✅ Ready to run pytest!"
        echo ""
        echo "Run tests with:"
        echo "  pytest sources/github/test/ -v"
        echo ""
    else
        echo ""
        echo "⚠️  Connection test failed. Check:"
        echo "  1. Cluster is running"
        echo "  2. Token is valid"
        echo "  3. Cluster ID is correct"
        echo ""
    fi
else
    echo "⚠️  databricks-connect not installed"
    echo ""
    echo "Install with:"
    echo "  pip install databricks-connect==14.3.*  # For DBR 14.x"
    echo "  pip install databricks-connect==13.3.*  # For DBR 13.x"
    echo ""
fi

echo "📚 For more information, see TESTING.md"
