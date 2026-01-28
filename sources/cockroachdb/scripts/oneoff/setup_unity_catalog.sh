#!/usr/bin/env bash
#
# Setup Unity Catalog Storage Credential and External Locations for CockroachDB CDC
# Uses Databricks CLI to create resources via REST API
#
# Prerequisites:
#   1. Databricks CLI installed and configured: https://docs.databricks.com/dev-tools/cli/
#   2. Azure credentials in cockroachdb_cdc_azure.json
#   3. Unity Catalog enabled in your workspace
#   4. Account admin or metastore admin privileges
#
# Usage:
#   ./setup_unity_catalog.sh
#

set -euo pipefail

# Find git root
if command -v git &> /dev/null && git rev-parse --show-toplevel &> /dev/null 2>&1; then
    GIT_ROOT="$(git rev-parse --show-toplevel)"
else
    GIT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Check for required tools
if ! command -v databricks &> /dev/null; then
    echo "❌ Error: Databricks CLI not found"
    echo ""
    echo "📦 Install Databricks CLI:"
    echo "   pip install databricks-cli"
    echo ""
    echo "🔧 Configure Databricks CLI:"
    echo "   databricks configure --token"
    echo "   Enter your workspace URL and personal access token"
    echo ""
    exit 1
fi

if ! command -v jq &> /dev/null; then
    echo "❌ Error: jq not found"
    echo "   Install: brew install jq"
    exit 1
fi

# Load Azure credentials
AZURE_JSON="$GIT_ROOT/sources/cockroachdb/.env/cockroachdb_cdc_azure.json"

if [[ ! -f "$AZURE_JSON" ]]; then
    echo "❌ Error: Azure credentials not found at:"
    echo "   $AZURE_JSON"
    echo ""
    echo "💡 Create credentials file first:"
    echo "   cd $GIT_ROOT/sources/cockroachdb/scripts"
    echo "   ./setup_azure_blob_for_cdc.sh"
    exit 1
fi

# Parse Azure credentials
AZURE_STORAGE_ACCOUNT=$(jq -r '.azure_storage_account' "$AZURE_JSON")
AZURE_STORAGE_KEY=$(jq -r '.azure_storage_key' "$AZURE_JSON")
AZURE_CONTAINER=$(jq -r '.azure_storage_container // "changefeed-events"' "$AZURE_JSON")

# Check for managed identity configuration
MANAGED_IDENTITY_TYPE=$(jq -r '.managed_identity.type // "none"' "$AZURE_JSON")
MANAGED_IDENTITY_CLIENT_ID=$(jq -r '.managed_identity.client_id // ""' "$AZURE_JSON")
MANAGED_IDENTITY_RESOURCE_ID=$(jq -r '.managed_identity.resource_id // ""' "$AZURE_JSON")
ACCESS_CONNECTOR_ID=$(jq -r '.access_connector_id // ""' "$AZURE_JSON")

# Configuration
STORAGE_CREDENTIAL_NAME="cockroachdb_cdc_storage_credential"
PARQUET_LOCATION_NAME="cockroachdb_cdc_parquet"
JSON_LOCATION_NAME="cockroachdb_cdc_json"

# Test Databricks CLI connectivity
echo "🔍 Testing Databricks CLI connectivity..."
if ! timeout 10 databricks current-user me --output json &>/dev/null; then
    echo "❌ Error: Cannot connect to Databricks"
    echo ""
    echo "📋 Databricks CLI Setup Required:"
    echo "   1. Install: pip install databricks-cli"
    echo "   2. Configure: databricks configure --token"
    echo "   3. Enter your workspace URL and personal access token"
    echo ""
    echo "💡 Get your access token:"
    echo "   Workspace → Settings → User Settings → Access Tokens → Generate New Token"
    echo ""
    exit 1
fi
echo "✅ Databricks CLI connected"
echo ""

# Get current user email
CURRENT_USER=$(databricks current-user me --output json 2>/dev/null | jq -r '.userName' || echo "")

if [[ -z "$CURRENT_USER" ]]; then
    echo "⚠️  Could not auto-detect user email"
    echo "   Please enter your Databricks user email:"
    read -r CURRENT_USER
fi

echo "================================================================================"
echo "UNITY CATALOG SETUP FOR COCKROACHDB CDC (ADLS Gen2)"
echo "================================================================================"
echo "Storage Account: $AZURE_STORAGE_ACCOUNT"
echo "Container: $AZURE_CONTAINER"
echo "User: $CURRENT_USER"
echo "Protocol: abfss:// (Azure Data Lake Storage Gen2)"
echo "================================================================================"
echo ""

# Step 1: Create Storage Credential
echo "📦 Step 1: Creating Storage Credential..."
echo ""

# Check if credential already exists
if databricks storage-credentials get "$STORAGE_CREDENTIAL_NAME" &>/dev/null; then
    echo "✅ Storage credential already exists: $STORAGE_CREDENTIAL_NAME"
else
    # Determine authentication method (handle both user-assigned and user_assigned)
    if [[ "$MANAGED_IDENTITY_TYPE" == "user-assigned" || "$MANAGED_IDENTITY_TYPE" == "user_assigned" ]] && [[ -n "$MANAGED_IDENTITY_CLIENT_ID" ]]; then
        echo "🔐 Creating storage credential with Azure Managed Identity (user-assigned)..."
        echo "   Client ID: $MANAGED_IDENTITY_CLIENT_ID"
        echo ""
        
        # Create storage credential with managed identity
        # Use Access Connector ID if available, otherwise fall back to managed identity resource ID
        CONNECTOR_ID="${ACCESS_CONNECTOR_ID:-$MANAGED_IDENTITY_RESOURCE_ID}"
        
        cat <<EOF > /tmp/storage_credential.json
{
  "name": "$STORAGE_CREDENTIAL_NAME",
  "comment": "Azure ADLS Gen2 credentials for CockroachDB CDC changefeeds (Managed Identity)",
  "azure_managed_identity": {
    "access_connector_id": "$CONNECTOR_ID"
  },
  "read_only": false,
  "skip_validation": false
}
EOF
        
        # Create via Databricks CLI
        echo "   Attempting to create via Databricks CLI (timeout: 30s)..."
        echo ""
        
        # Test Databricks CLI connectivity first
        if ! timeout 5 databricks current-user me --output json &>/dev/null; then
            echo "   ❌ FAILED: Cannot connect to Databricks"
            echo ""
            echo "   💡 Troubleshooting:"
            echo "      1. Check if Databricks CLI is configured:"
            echo "         databricks configure --token"
            echo "      2. Verify your workspace URL and token"
            echo "      3. Test connection:"
            echo "         databricks current-user me"
            echo ""
            CLI_EXIT_CODE=1
            CLI_OUTPUT="Databricks CLI authentication failed"
        else
            # Show JSON being sent for debugging
            echo "   📄 JSON payload:"
            cat /tmp/storage_credential.json | jq -C '.' 2>/dev/null || cat /tmp/storage_credential.json
            echo ""
            
            # Try to create the credential - capture output immediately for debugging
            echo "   🚀 Executing Databricks CLI command..."
            echo "   💡 If this hangs, the issue is with Databricks CLI or API endpoint"
            echo ""
            
            TMP_OUTPUT=$(mktemp)
            
            # Try with verbose timeout that shows what's happening
            echo "   ⏱️  Timeout: 30 seconds | Press Ctrl+C to abort"
            timeout --foreground 30 databricks storage-credentials create --json @/tmp/storage_credential.json > "$TMP_OUTPUT" 2>&1
            CLI_EXIT_CODE=$?
            
            CLI_OUTPUT=$(cat "$TMP_OUTPUT" 2>/dev/null || echo "No output captured")
            rm -f "$TMP_OUTPUT"
            
            # Check results
            if [ $CLI_EXIT_CODE -eq 124 ]; then
                echo "   ❌ FAILED: Command timed out after 30 seconds"
                echo ""
                echo "   💡 Possible causes:"
                echo "      • Databricks workspace is unreachable"
                echo "      • API endpoint is down or slow"
                echo "      • Network connectivity issues"
                echo "      • Authentication token expired"
                echo ""
                CLI_OUTPUT="Command timed out. Databricks API did not respond within 30 seconds."
            elif [ $CLI_EXIT_CODE -eq 130 ]; then
                echo "   ⚠️  Interrupted by user (Ctrl+C)"
                CLI_OUTPUT="User interrupted the operation"
            fi
        fi
        echo ""
        
        if [ $CLI_EXIT_CODE -eq 0 ]; then
            echo "   ✅ SUCCESS: Storage credential created via Databricks CLI"
            echo "   Name: $STORAGE_CREDENTIAL_NAME"
            echo "   Type: Azure Managed Identity (user-assigned)"
            echo ""
        else
            echo "   ❌ FAILED: Databricks CLI creation unsuccessful"
            echo ""
            
            # Show the actual error for debugging
            if [[ -n "$CLI_OUTPUT" ]]; then
                echo "   Error details:"
                echo "$CLI_OUTPUT" | head -10 | sed 's/^/   > /'
                echo ""
            fi
            
            # Provide helpful hints based on error
            if [[ "$CLI_OUTPUT" == *"RESOURCE_ALREADY_EXISTS"* ]]; then
                echo "   💡 Hint: Credential may already exist"
                echo "      Run: databricks storage-credentials get $STORAGE_CREDENTIAL_NAME"
                echo ""
            elif [[ "$CLI_OUTPUT" == *"access_connector"* ]] || [[ "$CLI_OUTPUT" == *"AccessConnector"* ]]; then
                echo "   💡 Hint: Databricks requires an Azure Access Connector (not a managed identity directly)"
                echo "      The managed identity must be associated with a Databricks Access Connector"
                echo "      See: https://learn.microsoft.com/azure/databricks/security/access-connectors"
                echo ""
            elif [[ "$CLI_OUTPUT" == *"INVALID_PARAMETER"* ]]; then
                echo "   💡 Hint: The resource ID format may be incorrect"
                echo "      Expected: /subscriptions/.../resourceGroups/.../providers/Microsoft.Databricks/accessConnectors/..."
                echo ""
            fi
            
            echo "📋 Manual Creation Options:"
            echo ""
            echo "   SQL Command (recommended):"
            echo "   CREATE STORAGE CREDENTIAL $STORAGE_CREDENTIAL_NAME"
            echo "   WITH ("
            echo "     AZURE_MANAGED_IDENTITY ("
            echo "       MANAGED_IDENTITY_ID '$MANAGED_IDENTITY_CLIENT_ID'"
            echo "     )"
            echo "   );"
            echo ""
            echo "   OR via Databricks UI:"
            echo "   1. Go to: Catalog → External Data → Storage Credentials"
            echo "   2. Click 'Create Credential'"
            echo "   3. Enter:"
            echo "      • Name: $STORAGE_CREDENTIAL_NAME"
            echo "      • Type: Azure Managed Identity"
            echo "      • Managed Identity Client ID: $MANAGED_IDENTITY_CLIENT_ID"
            echo "   4. Click 'Create'"
            echo ""
            echo "⏸️  Press Enter after creating the storage credential, or Ctrl+C to exit..."
            read -r
        fi
        
    elif [[ "$MANAGED_IDENTITY_TYPE" == "system-assigned" || "$MANAGED_IDENTITY_TYPE" == "system_assigned" ]]; then
        echo "⚠️  System-assigned managed identity detected"
        echo "    System-assigned identities must be configured at the cluster/SQL warehouse level"
        echo "    and cannot be used for Unity Catalog storage credentials."
        echo ""
        echo "💡 Recommendation: Use user-assigned managed identity instead"
        echo "   Run: ./setup_azure_blob_for_cdc.sh to reconfigure"
        echo ""
        exit 1
        
    else
        # No managed identity - use access key (manual UI creation required)
        echo "⚠️  Storage credential creation requires Azure authentication"
        echo ""
        echo "📋 OPTION 1: Create via Databricks UI (Access Key)"
        echo "   1. Go to: https://<your-workspace>.cloud.databricks.com/"
        echo "   2. Navigate to: Catalog → External Data → Storage Credentials"
        echo "   3. Click 'Create Credential'"
        echo "   4. Enter:"
        echo "      • Name: $STORAGE_CREDENTIAL_NAME"
        echo "      • Type: Azure Blob Storage"
        echo "      • Storage Account Name: $AZURE_STORAGE_ACCOUNT"
        echo "      • Access Key: <paste from JSON file>"
        echo "      • Comment: Azure Blob Storage credentials for CockroachDB CDC"
        echo "   5. Click 'Create'"
        echo ""
        echo "📋 OPTION 2: Configure Managed Identity (Recommended)"
        echo "   Run: ./setup_azure_blob_for_cdc.sh"
        echo "   This will add managed identity configuration to your Azure setup"
        echo ""
        echo "📋 Your Azure Storage Key (for manual UI entry):"
        echo "   $AZURE_STORAGE_KEY"
        echo ""
        
        # Ask user if they want to continue
        echo "⏸️  Press Enter after creating the storage credential via UI, or Ctrl+C to exit..."
        read -r
    fi
    
    # Verify credential was created
    if ! databricks storage-credentials get "$STORAGE_CREDENTIAL_NAME" &>/dev/null; then
        echo "❌ Storage credential still not found. Exiting."
        exit 1
    fi
    
    echo "✅ Storage credential verified: $STORAGE_CREDENTIAL_NAME"
fi

echo ""

# Step 2: Create External Locations
echo "📁 Step 2: Creating External Locations..."
echo ""

# Parquet location (using ADLS Gen2 abfss:// protocol)
PARQUET_URL="abfss://$AZURE_CONTAINER@$AZURE_STORAGE_ACCOUNT.dfs.core.windows.net/parquet-cdc"

if databricks external-locations get "$PARQUET_LOCATION_NAME" &>/dev/null; then
    echo "✅ External location already exists: $PARQUET_LOCATION_NAME"
else
    echo "   Creating: $PARQUET_LOCATION_NAME"
    if databricks external-locations create \
        --name "$PARQUET_LOCATION_NAME" \
        --url "$PARQUET_URL" \
        --credential-name "$STORAGE_CREDENTIAL_NAME" \
        --comment "CockroachDB CDC Parquet changefeeds from Azure Blob Storage" \
        --read-only false \
        --skip-validation false &>/dev/null; then
        echo "   ✅ SUCCESS: Created external location: $PARQUET_LOCATION_NAME"
    else
        echo "   ❌ FAILED: Could not create external location: $PARQUET_LOCATION_NAME"
        echo "   (Storage credential may not exist or permissions issue)"
    fi
fi

# JSON location (using ADLS Gen2 abfss:// protocol)
JSON_URL="abfss://$AZURE_CONTAINER@$AZURE_STORAGE_ACCOUNT.dfs.core.windows.net/json-cdc"

if databricks external-locations get "$JSON_LOCATION_NAME" &>/dev/null; then
    echo "✅ External location already exists: $JSON_LOCATION_NAME"
else
    echo "   Creating: $JSON_LOCATION_NAME"
    if databricks external-locations create \
        --name "$JSON_LOCATION_NAME" \
        --url "$JSON_URL" \
        --credential-name "$STORAGE_CREDENTIAL_NAME" \
        --comment "CockroachDB CDC JSON changefeeds from Azure Blob Storage" \
        --read-only false \
        --skip-validation false &>/dev/null; then
        echo "   ✅ SUCCESS: Created external location: $JSON_LOCATION_NAME"
    else
        echo "   ❌ FAILED: Could not create external location: $JSON_LOCATION_NAME"
        echo "   (Storage credential may not exist or permissions issue)"
    fi
fi

echo ""

# Step 3: Grant Permissions
echo "🔐 Step 3: Granting Permissions..."
echo ""

# Grant READ FILES on Parquet location
echo "   Granting READ FILES on: $PARQUET_LOCATION_NAME"
if databricks grants update \
    --securable-type EXTERNAL_LOCATION \
    --name "$PARQUET_LOCATION_NAME" \
    --principal "$CURRENT_USER" \
    --changes '[{"add": ["READ_FILES"]}]' &>/dev/null; then
    echo "   ✅ SUCCESS: Granted permissions to $CURRENT_USER"
else
    echo "   ⚠️  Grant may already exist or permission issue"
fi

# Grant READ FILES on JSON location
echo "   Granting READ FILES on: $JSON_LOCATION_NAME"
if databricks grants update \
    --securable-type EXTERNAL_LOCATION \
    --name "$JSON_LOCATION_NAME" \
    --principal "$CURRENT_USER" \
    --changes '[{"add": ["READ_FILES"]}]' &>/dev/null; then
    echo "   ✅ SUCCESS: Granted permissions to $CURRENT_USER"
else
    echo "   ⚠️  Grant may already exist or permission issue"
fi

echo ""

# Step 4: Verify Setup
echo "✅ Step 4: Verifying Setup..."
echo ""

echo "📋 Storage Credential:"
databricks storage-credentials get "$STORAGE_CREDENTIAL_NAME" --output json | jq -r '.name, .comment'
echo ""

echo "📋 External Locations:"
databricks external-locations get "$PARQUET_LOCATION_NAME" --output json | jq -r '{name, url, credential_name}'
databricks external-locations get "$JSON_LOCATION_NAME" --output json | jq -r '{name, url, credential_name}'
echo ""

echo "📋 Permissions:"
databricks grants get \
    --securable-type EXTERNAL_LOCATION \
    --name "$PARQUET_LOCATION_NAME" --output json 2>/dev/null | jq -r '.privilege_assignments[]' || echo "  (No permissions to view grants)"

echo ""
echo "================================================================================"
echo "✅ SETUP COMPLETE!"
echo "================================================================================"
echo ""

# Show SQL commands summary for verification
echo "📋 SQL Commands Used (for verification):"
echo "================================================================================"
echo ""
echo "1️⃣  Storage Credential:"
if [[ "$MANAGED_IDENTITY_TYPE" == "user-assigned" || "$MANAGED_IDENTITY_TYPE" == "user_assigned" ]]; then
    echo "   CREATE STORAGE CREDENTIAL $STORAGE_CREDENTIAL_NAME"
    echo "   WITH ("
    echo "     AZURE_MANAGED_IDENTITY ("
    echo "       MANAGED_IDENTITY_ID '$MANAGED_IDENTITY_CLIENT_ID'"
    echo "     )"
    echo "   );"
else
    echo "   -- Created via Databricks UI with access key"
fi
echo ""

echo "2️⃣  External Location - Parquet:"
echo "   CREATE EXTERNAL LOCATION $PARQUET_LOCATION_NAME"
echo "   URL '$PARQUET_URL'"
echo "   WITH (STORAGE CREDENTIAL $STORAGE_CREDENTIAL_NAME);"
echo ""

echo "3️⃣  External Location - JSON:"
echo "   CREATE EXTERNAL LOCATION $JSON_LOCATION_NAME"
echo "   URL '$JSON_URL'"
echo "   WITH (STORAGE CREDENTIAL $STORAGE_CREDENTIAL_NAME);"
echo ""

echo "4️⃣  Permissions:"
echo "   GRANT READ FILES ON EXTERNAL LOCATION $PARQUET_LOCATION_NAME TO \`$CURRENT_USER\`;"
echo "   GRANT READ FILES ON EXTERNAL LOCATION $JSON_LOCATION_NAME TO \`$CURRENT_USER\`;"
echo ""
echo "================================================================================"
echo ""

echo "💡 Next Steps:"
echo "   1. Open your Databricks notebook"
echo "   2. Use Unity Catalog external location names:"
echo "      • azure-blob://$PARQUET_LOCATION_NAME"
echo "      • azure-blob://$JSON_LOCATION_NAME"
echo "   3. Authentication will be automatic via Unity Catalog"
echo ""
echo "🔍 Verify Access (in Databricks notebook):"
echo "   # List files using external location"
echo "   files = dbutils.fs.ls('azure-blob://$PARQUET_LOCATION_NAME/')"
echo "   print(f'Found {len(files)} files')"
echo ""
echo "   # Or use direct abfss:// URL (credentials resolved by Unity Catalog)"
echo "   df = spark.read.parquet('$PARQUET_URL/')"
echo "   display(df)"
echo ""
echo "================================================================================"

