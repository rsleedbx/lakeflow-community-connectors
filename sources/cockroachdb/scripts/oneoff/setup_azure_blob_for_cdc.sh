#!/usr/bin/env bash
# Azure CLI commands to create ADLS Gen2 Storage for CockroachDB Changefeed
# Creates an Azure Data Lake Storage Gen2 account with hierarchical namespace
# enabled for abfss:// protocol support

set -u

# Get git root first to check for existing credentials
GIT_ROOT="$(git rev-parse --show-toplevel 2>/dev/null)"
if [ -z "$GIT_ROOT" ]; then
    echo "❌ Not in a git repository"
    exit 1
fi

COCKROACH_DIR="$GIT_ROOT/sources/cockroachdb"
ENV_DIR="$COCKROACH_DIR/.env"
JSON_FILE="$ENV_DIR/cockroachdb_cdc_azure.json"

# Global associative array for credentials (Python-style dictionary in bash)
declare -A credentials

# Default configuration
RESOURCE_GROUP="cockroachdb-cdc-rg"
CONTAINER_NAME="changefeed-events"

# Check if credentials already exist in JSON
if [ -f "$JSON_FILE" ]; then
    echo "✅ Found existing Azure credentials: $JSON_FILE"
    echo ""
    
    # Check if yq is available
    if ! command -v yq &> /dev/null; then
        echo "❌ Error: yq is required to parse JSON credentials"
        echo "   Install with: brew install yq (macOS) or snap install yq (Linux)"
        exit 1
    fi
    
    # Load JSON credentials into global associative array
    while IFS='=' read -r key value; do
        value="${value%\'}"
        value="${value#\'}"
        credentials["$key"]="$value"
    done < <(yq -o=shell "$JSON_FILE")
    
    echo "📋 Reusing existing configuration from JSON:"
    echo "  Storage Account: ${credentials[azure_storage_account]}"
    echo "  Container: ${credentials[azure_storage_container]}"
    echo ""
    echo "⏭️  Skipping Azure resource checks (JSON exists)"
    echo ""
    
    # Skip to displaying results
    SKIP_CREATION=true
    SKIP_JSON_SAVE=true
else
    echo "🔧 Setting up Azure Blob Storage for CockroachDB Changefeed"
    echo "============================================================"
    echo ""
    
    # Check if Azure resources already exist
    echo "🔍 Checking for existing Azure resources..."
    echo ""
    
    # Check if resource group exists
    RG_EXISTS=$(az group exists --name "$RESOURCE_GROUP")
    
    if [ "$RG_EXISTS" = "true" ]; then
        echo "  ✅ Resource group exists: $RESOURCE_GROUP"
        
        # List storage accounts in the resource group
        EXISTING_ACCOUNTS=$(az storage account list \
            --resource-group "$RESOURCE_GROUP" \
            --query "[?starts_with(name, 'cockroachcdc')].name" \
            --output tsv 2>/dev/null)
        
        if [ -n "$EXISTING_ACCOUNTS" ]; then
            # Use the first matching storage account
            STORAGE_ACCOUNT=$(echo "$EXISTING_ACCOUNTS" | head -1)
            echo "  ✅ Found existing storage account: $STORAGE_ACCOUNT"
            
            credentials[resource_group]="$RESOURCE_GROUP"
            credentials[azure_storage_account]="$STORAGE_ACCOUNT"
            credentials[azure_storage_container]="$CONTAINER_NAME"
            
            SKIP_CREATION=true
            SKIP_JSON_SAVE=false  # Need to create JSON with existing resources
        else
            echo "  ℹ️  No storage account found in resource group"
            credentials[resource_group]="$RESOURCE_GROUP"
            credentials[azure_storage_account]="cockroachcdc$(date +%s)"
            credentials[azure_storage_container]="$CONTAINER_NAME"
            SKIP_CREATION=false
            SKIP_JSON_SAVE=false
        fi
    else
        echo "  ℹ️  Resource group does not exist: $RESOURCE_GROUP"
        credentials[resource_group]="$RESOURCE_GROUP"
        credentials[azure_storage_account]="cockroachcdc$(date +%s)"
        credentials[azure_storage_container]="$CONTAINER_NAME"
        SKIP_CREATION=false
        SKIP_JSON_SAVE=false
    fi
    
    echo ""
    echo "Configuration:"
    echo "  Resource Group: ${credentials[resource_group]}"
    echo "  Storage Account: ${credentials[azure_storage_account]}"
    echo "  Container: ${credentials[azure_storage_container]}"
    echo ""
fi

if [ "$SKIP_CREATION" = false ]; then
    # Check if jq is available (needed for JSON merging)
    if ! command -v jq &> /dev/null; then
        echo "❌ Error: jq is required to update JSON credentials"
        echo "   Install with: brew install jq (macOS) or apt-get install jq (Linux)"
        exit 1
    fi
    
    # Get default location from Azure CLI config
    CLOUD_LOCATION=${CLOUD_LOCATION:-''}
    if [[ -z $CLOUD_LOCATION ]]; then 
        CLOUD_LOCATION=$(az config get defaults.location --query value -o tsv 2>/dev/null)
    fi

    if [ -z "$CLOUD_LOCATION" ]; then
        # Fall back to eastus if no default is configured
        CLOUD_LOCATION="eastus"
        echo "  ℹ️  No default location configured, using: $CLOUD_LOCATION"
        echo "  💡 To set a default: az configure --defaults location=<your-region>"
    else
        echo "  ✅ Using configured default location: $CLOUD_LOCATION"
    fi
    echo ""
    
    # Step 1: Create resource group (if it doesn't exist)
    echo "Step 1: Creating resource group..."


    # create resource group
    if ! AZ group show --resource-group "${credentials[resource_group]}" ; then
        # multiples tags are defined correctly below.  NOT A MISTAKE
        DB_EXIT_ON_ERROR="PRINT_EXIT" AZ group create --resource-group "${credentials[resource_group]}" \
            --tags "Owner=${DBX_USERNAME}" "${REMOVE_AFTER:+RemoveAfter=${REMOVE_AFTER}}"
    fi

    echo ""

    # Step 2: Create storage account with ADLS Gen2 enabled (hierarchical namespace)
    echo "Step 2: Creating ADLS Gen2 storage account..."
    echo "  ℹ️  Enabling hierarchical namespace for abfss:// support"
    az storage account create \
      --name "${credentials[azure_storage_account]}" \
      --resource-group "${credentials[resource_group]}" \
      --sku Standard_LRS \
      --kind StorageV2 \
      --access-tier Hot \
      --allow-blob-public-access false \
      --enable-hierarchical-namespace true \
      --location "${CLOUD_LOCATION}" \
      --output table

    echo ""
fi

# Get storage key and container info (whether created new or reusing existing)
if [ "$SKIP_CREATION" = true ] && [ "$SKIP_JSON_SAVE" = false ]; then
    echo "📥 Retrieving credentials from existing Azure resources..."
    echo ""
fi

if [ "$SKIP_JSON_SAVE" = false ]; then
    # Get storage account key
    if [ "$SKIP_CREATION" = false ]; then
        echo "Step 3: Getting storage account key..."
    else
        echo "  Getting storage account key..."
    fi
    
    credentials[azure_storage_key]=$(az storage account keys list \
      --resource-group "${credentials[resource_group]}" \
      --account-name "${credentials[azure_storage_account]}" \
      --query '[0].value' \
      --output tsv)

    echo "  ✅ Storage key retrieved"
    echo ""

    # Create or verify blob container exists
    if [ "$SKIP_CREATION" = false ]; then
        echo "Step 4: Creating blob container..."
    else
        echo "  Ensuring blob container exists..."
    fi
    
    az storage container create \
      --name "${credentials[azure_storage_container]}" \
      --account-name "${credentials[azure_storage_account]}" \
      --account-key "${credentials[azure_storage_key]}" \
      --output table || echo "  ℹ️  Container already exists"

    echo ""

    # Get connection string
    if [ "$SKIP_CREATION" = false ]; then
        echo "Step 5: Getting connection string..."
    else
        echo "  Getting connection string..."
    fi
    
    credentials[azure_connection_string]=$(az storage account show-connection-string \
      --name "${credentials[azure_storage_account]}" \
      --resource-group "${credentials[resource_group]}" \
      --output tsv)

    echo "  ✅ Connection string retrieved"
    echo ""
    
    # Get storage account resource ID (needed for managed identity RBAC)
    if [ "$SKIP_CREATION" = false ]; then
        echo "Step 6: Getting storage account resource ID..."
    else
        echo "  Getting storage account resource ID..."
    fi
    
    credentials[storage_resource_id]=$(az storage account show \
      --name "${credentials[azure_storage_account]}" \
      --resource-group "${credentials[resource_group]}" \
      --query id \
      --output tsv)
    
    echo "  ✅ Resource ID: ${credentials[storage_resource_id]}"
    echo ""
    
    # Managed Identity Configuration (optional)
    echo "🔐 Managed Identity Configuration (optional):"
    echo "   Managed identities provide secure, keyless authentication to Azure services."
    echo "   This is the recommended authentication method for production workloads."
    echo ""
    echo "   Would you like to configure managed identity? (y/N)"
    read -r CONFIGURE_MI
    
    if [[ "$CONFIGURE_MI" =~ ^[Yy]$ ]]; then
        echo ""
        echo "📋 Managed Identity Options:"
        echo "   1) Use existing user-assigned managed identity"
        echo "   2) Create new user-assigned managed identity"
        echo "   3) Use system-assigned managed identity (requires VM/container context)"
        echo "   4) Skip managed identity configuration"
        echo ""
        echo "   Enter choice (1-4):"
        read -r MI_CHOICE
        
        case $MI_CHOICE in
            1)
                echo ""
                echo "Enter the name of the existing managed identity:"
                read -r MI_NAME
                echo "Enter the resource group of the managed identity:"
                read -r MI_RESOURCE_GROUP
                
                # Get managed identity details
                MI_DETAILS=$(az identity show \
                  --name "$MI_NAME" \
                  --resource-group "$MI_RESOURCE_GROUP" \
                  --output json 2>/dev/null)
                
                if [ -n "$MI_DETAILS" ]; then
                    credentials[managed_identity_type]="user_assigned"
                    credentials[managed_identity_client_id]=$(echo "$MI_DETAILS" | jq -r '.clientId')
                    credentials[managed_identity_resource_id]=$(echo "$MI_DETAILS" | jq -r '.id')
                    credentials[managed_identity_principal_id]=$(echo "$MI_DETAILS" | jq -r '.principalId')
                    
                    echo "  ✅ Found managed identity: $MI_NAME"
                    echo "     Client ID: ${credentials[managed_identity_client_id]}"
                else
                    echo "  ❌ Managed identity not found: $MI_NAME"
                fi
                ;;
            2)
                echo ""
                echo "Enter name for new managed identity (e.g., cockroachdb-cdc-identity):"
                read -r MI_NAME
                
                if [ -z "$MI_NAME" ]; then
                    MI_NAME="cockroachdb-cdc-identity-$(date +%s)"
                fi
                
                echo "  Creating managed identity: $MI_NAME..."
                
                MI_DETAILS=$(az identity create \
                  --name "$MI_NAME" \
                  --resource-group "${credentials[resource_group]}" \
                  --output json)
                
                credentials[managed_identity_type]="user_assigned"
                credentials[managed_identity_client_id]=$(echo "$MI_DETAILS" | jq -r '.clientId')
                credentials[managed_identity_resource_id]=$(echo "$MI_DETAILS" | jq -r '.id')
                credentials[managed_identity_principal_id]=$(echo "$MI_DETAILS" | jq -r '.principalId')
                
                echo "  ✅ Created managed identity: $MI_NAME"
                echo "     Client ID: ${credentials[managed_identity_client_id]}"
                ;;
            3)
                echo ""
                echo "  ℹ️  System-assigned managed identity is automatically created with your VM/container."
                echo "     You'll need to retrieve its principal ID from your compute resource."
                echo ""
                echo "  Enter the principal ID (or leave empty to configure later):"
                read -r MI_PRINCIPAL_ID
                
                if [ -n "$MI_PRINCIPAL_ID" ]; then
                    credentials[managed_identity_type]="system_assigned"
                    credentials[managed_identity_principal_id]="$MI_PRINCIPAL_ID"
                    echo "  ✅ System-assigned managed identity configured"
                else
                    echo "  ⏭️  Skipping - configure manually later"
                fi
                ;;
            *)
                echo "  ⏭️  Skipping managed identity configuration"
                ;;
        esac
        
        # Assign RBAC roles if managed identity was configured
        if [ -n "${credentials[managed_identity_principal_id]:-}" ]; then
            echo ""
            echo "🔐 RBAC Role Assignment:"
            echo "   Would you like to assign storage roles to this managed identity? (Y/n)"
            read -r ASSIGN_ROLES
            
            if [[ ! "$ASSIGN_ROLES" =~ ^[Nn]$ ]]; then
                echo ""
                echo "  Assigning 'Storage Blob Data Contributor' role..."
                
                az role assignment create \
                  --role "Storage Blob Data Contributor" \
                  --assignee "${credentials[managed_identity_principal_id]}" \
                  --scope "${credentials[storage_resource_id]}" \
                  --output table || echo "  ℹ️  Role may already be assigned"
                
                echo "  ✅ Role assignment complete"
                echo ""
                echo "  💡 The managed identity now has read/write access to this storage account."
            fi
        fi
        
        echo ""
    else
        echo "  ⏭️  Skipping managed identity configuration (using access key authentication)"
        echo ""
    fi
fi

# Display results
echo "================================================================"
if [ "$SKIP_CREATION" = false ]; then
    echo "✅ Azure Blob Storage Created Successfully!"
elif [ "$SKIP_JSON_SAVE" = false ]; then
    echo "✅ Detected and Connected to Existing Azure Resources!"
else
    echo "✅ Using Existing Configuration"
fi
echo "================================================================"
echo ""
echo "📋 Configuration Details:"
echo "------------------------"
echo "Storage Account: ${credentials[azure_storage_account]}"
echo "Container Name: ${credentials[azure_storage_container]}"
if [ -n "${credentials[azure_storage_key]}" ]; then
    echo "Account Key: ${credentials[azure_storage_key]:0:20}..." # Show first 20 chars only
fi
echo ""

# Check/Configure Managed Identity (for existing or new configurations)
if [ "$SKIP_JSON_SAVE" = true ]; then
    CONFIGURE_MI_SECTION=false
    
    if [ -n "${credentials[managed_identity_type]:-}" ]; then
        # Managed identity already configured
        echo "🔐 Managed Identity Status:"
        echo "   ✅ Already configured: ${credentials[managed_identity_type]}"
        if [ -n "${credentials[managed_identity_client_id]:-}" ]; then
            echo "   Client ID: ${credentials[managed_identity_client_id]}"
        fi
        echo ""
        echo "   Update managed identity configuration? (y/N)"
        read -r UPDATE_MI
        if [[ "$UPDATE_MI" =~ ^[Yy]$ ]]; then
            CONFIGURE_MI_SECTION=true
        else
            echo "   ⏭️  Keeping existing managed identity configuration"
            echo ""
        fi
    else
        # Existing config without managed identity - offer to add it
        echo "🔐 Managed Identity Configuration:"
        echo "   Your configuration uses access key authentication."
        echo "   Would you like to add managed identity for keyless authentication? (y/N)"
        read -r ADD_MI
        
        if [[ "$ADD_MI" =~ ^[Yy]$ ]]; then
            CONFIGURE_MI_SECTION=true
        fi
    fi
    
    # Run managed identity configuration if requested
    if [ "$CONFIGURE_MI_SECTION" = true ]; then
        # Get storage resource ID (and parse resource group from it if needed)
        echo "  ℹ️  Retrieving storage account information from Azure..."
        echo "     Storage Account: ${credentials[azure_storage_account]}"
        
        # Method 1: If we have resource group, use it
        if [ -n "${credentials[resource_group]:-}" ]; then
            echo "     Using existing resource group: ${credentials[resource_group]}"
            credentials[storage_resource_id]=$(az storage account show \
              --name "${credentials[azure_storage_account]}" \
              --resource-group "${credentials[resource_group]}" \
              --query id \
              --output tsv 2>/dev/null || echo "")
        else
            # Method 2: Search across all resource groups and get the resource ID
            echo "     Searching for storage account across all resource groups..."
            TEMP_RESOURCE_ID=$(az storage account list \
              --query "[?name=='${credentials[azure_storage_account]}'].id | [0]" \
              --output tsv 2>&1)
            
            # Check if command succeeded
            if [ $? -eq 0 ] && [ -n "$TEMP_RESOURCE_ID" ] && [[ ! "$TEMP_RESOURCE_ID" =~ ERROR ]] && [[ ! "$TEMP_RESOURCE_ID" =~ "null" ]]; then
                credentials[storage_resource_id]="$TEMP_RESOURCE_ID"
                echo "     ✅ Retrieved resource ID"
                
                # Parse resource group from ID: /subscriptions/{sub}/resourceGroups/{rg}/providers/...
                credentials[resource_group]=$(echo "$TEMP_RESOURCE_ID" | sed -E 's|.*/resourceGroups/([^/]+)/.*|\1|')
                
                if [ -n "${credentials[resource_group]}" ]; then
                    echo "     ✅ Parsed resource group from ID: ${credentials[resource_group]}"
                fi
            else
                echo "     ⚠️  Could not find storage account in current subscription"
            fi
        fi
        
        # Final check - ensure we have resource group (minimum requirement)
        if [ -z "${credentials[resource_group]:-}" ]; then
            echo "  ❌ Could not determine resource group from Azure"
            echo "     Please enter the resource group name manually (e.g., cockroachdb-cdc-rg):"
            read -r RG_NAME
            if [ -n "$RG_NAME" ]; then
                credentials[resource_group]="$RG_NAME"
                echo "     ✅ Using resource group: $RG_NAME"
                
                # Try to get resource ID with manual resource group
                echo "     Attempting to retrieve resource ID..."
                TEMP_RESOURCE_ID=$(az storage account show \
                  --name "${credentials[azure_storage_account]}" \
                  --resource-group "${credentials[resource_group]}" \
                  --query id \
                  --output tsv 2>/dev/null || echo "")
                
                if [ -n "$TEMP_RESOURCE_ID" ]; then
                    credentials[storage_resource_id]="$TEMP_RESOURCE_ID"
                    echo "     ✅ Retrieved resource ID"
                else
                    # Build resource ID manually if Azure query fails
                    SUBSCRIPTION_ID=$(az account show --query id --output tsv 2>/dev/null || echo "")
                    if [ -n "$SUBSCRIPTION_ID" ]; then
                        credentials[storage_resource_id]="/subscriptions/${SUBSCRIPTION_ID}/resourceGroups/${credentials[resource_group]}/providers/Microsoft.Storage/storageAccounts/${credentials[azure_storage_account]}"
                        echo "     ℹ️  Constructed resource ID manually"
                    fi
                fi
            fi
        fi
        
        # Proceed if we have at least the resource group
        if [ -n "${credentials[resource_group]:-}" ]; then
            # Run managed identity setup
            echo ""
            echo "📋 Managed Identity Options:"
            echo "   1) Use existing user-assigned managed identity"
            echo "   2) Create new user-assigned managed identity"
            echo "   3) Use system-assigned managed identity"
            echo "   4) Skip"
            echo ""
            echo "   Enter choice (1-4):"
            read -r MI_CHOICE
            
            case $MI_CHOICE in
                1)
                    echo ""
                    echo "Enter the name of the existing managed identity:"
                    read -r MI_NAME
                    echo "Enter the resource group of the managed identity:"
                    read -r MI_RESOURCE_GROUP
                    
                    MI_DETAILS=$(az identity show \
                      --name "$MI_NAME" \
                      --resource-group "$MI_RESOURCE_GROUP" \
                      --output json 2>/dev/null)
                    
                    if [ -n "$MI_DETAILS" ]; then
                        credentials[managed_identity_type]="user_assigned"
                        credentials[managed_identity_client_id]=$(echo "$MI_DETAILS" | jq -r '.clientId')
                        credentials[managed_identity_resource_id]=$(echo "$MI_DETAILS" | jq -r '.id')
                        credentials[managed_identity_principal_id]=$(echo "$MI_DETAILS" | jq -r '.principalId')
                        
                        echo "  ✅ Found managed identity: $MI_NAME"
                        echo "     Client ID: ${credentials[managed_identity_client_id]}"
                        
                        # Offer to assign roles
                        echo ""
                        echo "  Would you like to assign Storage Blob Data Contributor role? (Y/n)"
                        read -r ASSIGN_ROLE
                        if [[ ! "$ASSIGN_ROLE" =~ ^[Nn]$ ]]; then
                            az role assignment create \
                              --role "Storage Blob Data Contributor" \
                              --assignee "${credentials[managed_identity_principal_id]}" \
                              --scope "${credentials[storage_resource_id]}" \
                              --output table || echo "  ℹ️  Role may already be assigned"
                            echo "  ✅ Role assignment complete"
                        fi
                        
                        # Force JSON update
                        SKIP_JSON_SAVE=false
                    else
                        echo "  ❌ Managed identity not found: $MI_NAME"
                    fi
                    ;;
                2)
                    echo ""
                    echo "Enter name for new managed identity (or press Enter for auto-generated):"
                    read -r MI_NAME
                    
                    if [ -z "$MI_NAME" ]; then
                        MI_NAME="cockroachdb-cdc-identity-$(date +%s)"
                    fi
                    
                    echo "  Creating managed identity: $MI_NAME..."
                    
                    MI_DETAILS=$(az identity create \
                      --name "$MI_NAME" \
                      --resource-group "${credentials[resource_group]}" \
                      --output json)
                    
                    credentials[managed_identity_type]="user_assigned"
                    credentials[managed_identity_client_id]=$(echo "$MI_DETAILS" | jq -r '.clientId')
                    credentials[managed_identity_resource_id]=$(echo "$MI_DETAILS" | jq -r '.id')
                    credentials[managed_identity_principal_id]=$(echo "$MI_DETAILS" | jq -r '.principalId')
                    
                    echo "  ✅ Created managed identity: $MI_NAME"
                    echo "     Client ID: ${credentials[managed_identity_client_id]}"
                    
                    # Assign role
                    echo ""
                    echo "  Assigning Storage Blob Data Contributor role..."
                    az role assignment create \
                      --role "Storage Blob Data Contributor" \
                      --assignee "${credentials[managed_identity_principal_id]}" \
                      --scope "${credentials[storage_resource_id]}" \
                      --output table 2>/dev/null || echo "  ℹ️  Role may already be assigned"
                    echo "  ✅ Role assignment complete"
                    
                    # Create Access Connector for Databricks Unity Catalog
                    echo ""
                    echo "  📦 Creating Azure Databricks Access Connector..."
                    echo "     (Required for Unity Catalog with Managed Identity)"
                    
                    AC_NAME="cockroachdb-cdc-access-connector"
                    LOCATION="${CLOUD_LOCATION:-$(az config get defaults.location --query value -o tsv 2>/dev/null || echo "eastus")}"
                    
                    # Check if access connector already exists
                    AC_EXISTS=$(az databricks access-connector show \
                      --name "$AC_NAME" \
                      --resource-group "${credentials[resource_group]}" \
                      --query id -o tsv || echo "")
                    
                    if [ -n "$AC_EXISTS" ]; then
                        echo "  ✅ Access Connector already exists: $AC_NAME"
                        credentials[access_connector_id]="$AC_EXISTS"
                    else
                        # Create access connector
                        AC_RESULT=$(az databricks access-connector create \
                          --name "$AC_NAME" \
                          --resource-group "${credentials[resource_group]}" \
                          --location "$LOCATION" \
                          --identity-type UserAssigned \
                          --user-assigned-identities "${credentials[managed_identity_resource_id]}" \
                          --output json 2>&1)
                        
                        if [ $? -eq 0 ]; then
                            credentials[access_connector_id]=$(echo "$AC_RESULT" | jq -r '.id')
                            echo "  ✅ Created Access Connector: $AC_NAME"
                            echo "     ID: ${credentials[access_connector_id]}"
                        else
                            echo "  ⚠️  Could not create Access Connector"
                            echo "     This may require additional Azure permissions"
                            echo "     You can create it manually later via Azure Portal"
                            echo ""
                            echo "     Manual creation command:"
                            echo "     az databricks access-connector create \\"
                            echo "       --name $AC_NAME \\"
                            echo "       --resource-group ${credentials[resource_group]} \\"
                            echo "       --location $LOCATION \\"
                            echo "       --identity-type UserAssigned \\"
                            echo "       --user-assigned-identities ${credentials[managed_identity_resource_id]}"
                        fi
                    fi
                    
                    # Force JSON update
                    SKIP_JSON_SAVE=false
                    ;;
                3)
                    echo ""
                    echo "  Enter the principal ID of the system-assigned managed identity:"
                    read -r MI_PRINCIPAL_ID
                    
                    if [ -n "$MI_PRINCIPAL_ID" ]; then
                        credentials[managed_identity_type]="system_assigned"
                        credentials[managed_identity_principal_id]="$MI_PRINCIPAL_ID"
                        
                        # Offer to assign roles
                        echo ""
                        echo "  Would you like to assign Storage Blob Data Contributor role? (Y/n)"
                        read -r ASSIGN_ROLE
                        if [[ ! "$ASSIGN_ROLE" =~ ^[Nn]$ ]]; then
                            az role assignment create \
                              --role "Storage Blob Data Contributor" \
                              --assignee "${credentials[managed_identity_principal_id]}" \
                              --scope "${credentials[storage_resource_id]}" \
                              --output table || echo "  ℹ️  Role may already be assigned"
                            echo "  ✅ Role assignment complete"
                        fi
                        
                        # Force JSON update
                        SKIP_JSON_SAVE=false
                    fi
                    ;;
                *)
                    echo "  ⏭️  Skipping managed identity configuration"
                    ;;
            esac
        else
            echo ""
            echo "  ⚠️  Skipping managed identity setup (resource group required)"
        fi  # End of if [ -n "${credentials[resource_group]:-}" ]
        echo ""
    fi  # End of if [ "$CONFIGURE_MI_SECTION" = true ]
fi  # End of if [ "$SKIP_JSON_SAVE" = true ]

# Save credentials to JSON file (if needed)
if [ "$SKIP_JSON_SAVE" = false ]; then
    echo "💾 Saving credentials to JSON file..."

    # Create .env directory if it doesn't exist
    mkdir -p "$ENV_DIR"

    # Check if jq is available (needed for JSON merging)
    if ! command -v jq &> /dev/null; then
        echo "❌ Error: jq is required to update JSON credentials"
        echo "   Install with: brew install jq (macOS) or apt-get install jq (Linux)"
        exit 1
    fi

    # Build changefeed URI and URLs
    credentials[changefeed_uri]="azure-blob://${credentials[azure_storage_container]}?AZURE_ACCOUNT_NAME=${credentials[azure_storage_account]}&AZURE_ACCOUNT_KEY=${credentials[azure_storage_key]}"
    
    # Build ADLS Gen2 URLs (abfss:// protocol)
    credentials[abfss_base_url]="abfss://${credentials[azure_storage_container]}@${credentials[azure_storage_account]}.dfs.core.windows.net"
    credentials[abfss_parquet_url]="abfss://${credentials[azure_storage_container]}@${credentials[azure_storage_account]}.dfs.core.windows.net/parquet-cdc"
    credentials[abfss_json_url]="abfss://${credentials[azure_storage_container]}@${credentials[azure_storage_account]}.dfs.core.windows.net/json-cdc"
    
    # Build legacy Blob Storage URLs (wasbs:// protocol)
    credentials[wasbs_base_url]="wasbs://${credentials[azure_storage_container]}@${credentials[azure_storage_account]}.blob.core.windows.net"
    credentials[wasbs_parquet_url]="wasbs://${credentials[azure_storage_container]}@${credentials[azure_storage_account]}.blob.core.windows.net/parquet-cdc"
    credentials[wasbs_json_url]="wasbs://${credentials[azure_storage_container]}@${credentials[azure_storage_account]}.blob.core.windows.net/json-cdc"
    
    # Check if file already exists to preserve user modifications
    if [ -f "$JSON_FILE" ]; then
        echo "  ℹ️  Existing file found - preserving user modifications..."
        
        # Merge new Azure credentials with existing JSON using jq
        TEMP_FILE=$(mktemp)
        
        # Build jq arguments
        JQ_ARGS=(
            --arg account "${credentials[azure_storage_account]}"
            --arg key "${credentials[azure_storage_key]}"
            --arg container "${credentials[azure_storage_container]}"
            --arg resource_group "${credentials[resource_group]:-}"
            --arg uri "${credentials[changefeed_uri]}"
            --arg abfss_base "${credentials[abfss_base_url]}"
            --arg abfss_parquet "${credentials[abfss_parquet_url]}"
            --arg abfss_json "${credentials[abfss_json_url]}"
            --arg wasbs_base "${credentials[wasbs_base_url]}"
            --arg wasbs_parquet "${credentials[wasbs_parquet_url]}"
            --arg wasbs_json "${credentials[wasbs_json_url]}"
            --arg storage_resource_id "${credentials[storage_resource_id]:-}"
            --arg mi_type "${credentials[managed_identity_type]:-}"
            --arg mi_client_id "${credentials[managed_identity_client_id]:-}"
            --arg mi_resource_id "${credentials[managed_identity_resource_id]:-}"
            --arg mi_principal_id "${credentials[managed_identity_principal_id]:-}"
            --arg access_connector_id "${credentials[access_connector_id]:-}"
        )
        
        jq "${JQ_ARGS[@]}" '. + {
             "_comment": "Auto-generated by setup_azure_blob_for_cdc.sh",
             "_description": "Azure Blob Storage credentials for CockroachDB CDC changefeeds",
             "azure_storage_account": $account,
             "azure_storage_key": $key,
             "azure_storage_container": $container,
             "resource_group": $resource_group,
             "changefeed_uri": $uri,
             "abfss_base_url": $abfss_base,
             "abfss_parquet_url": $abfss_parquet,
             "abfss_json_url": $abfss_json,
             "wasbs_base_url": $wasbs_base,
             "wasbs_parquet_url": $wasbs_parquet,
             "wasbs_json_url": $wasbs_json,
             "storage_resource_id": $storage_resource_id
           } + (if $mi_type != "" then {
             "managed_identity": {
               "type": $mi_type,
               "client_id": $mi_client_id,
               "resource_id": $mi_resource_id,
               "principal_id": $mi_principal_id
             },
             "access_connector_id": $access_connector_id
           } else {} end)' "$JSON_FILE" > "$TEMP_FILE"
        
        mv "$TEMP_FILE" "$JSON_FILE"
        echo "  ✅ Azure credentials updated (existing fields preserved)"
    else
        # Create new file
        cat > "$JSON_FILE" << EOF
{
  "_comment": "Auto-generated by setup_azure_blob_for_cdc.sh",
  "_description": "Azure Blob Storage credentials for CockroachDB CDC changefeeds",
  
  "azure_storage_account": "${credentials[azure_storage_account]}",
  "azure_storage_key": "${credentials[azure_storage_key]}",
  "azure_storage_container": "${credentials[azure_storage_container]}",
  "resource_group": "${credentials[resource_group]:-}",
  "changefeed_uri": "${credentials[changefeed_uri]}",
  
  "abfss_base_url": "${credentials[abfss_base_url]}",
  "abfss_parquet_url": "${credentials[abfss_parquet_url]}",
  "abfss_json_url": "${credentials[abfss_json_url]}",
  
  "wasbs_base_url": "${credentials[wasbs_base_url]}",
  "wasbs_parquet_url": "${credentials[wasbs_parquet_url]}",
  "wasbs_json_url": "${credentials[wasbs_json_url]}",
  
  "storage_resource_id": "${credentials[storage_resource_id]:-}"
EOF

        # Add managed identity section if configured
        if [ -n "${credentials[managed_identity_type]:-}" ]; then
            cat >> "$JSON_FILE" << EOF
,
  "managed_identity": {
    "type": "${credentials[managed_identity_type]}",
    "client_id": "${credentials[managed_identity_client_id]:-}",
    "resource_id": "${credentials[managed_identity_resource_id]:-}",
    "principal_id": "${credentials[managed_identity_principal_id]:-}"
  },
  "access_connector_id": "${credentials[access_connector_id]:-}"
EOF
        fi
        
        # Close JSON
        echo "}" >> "$JSON_FILE"
        
        echo "  ✅ JSON credentials file created: $JSON_FILE"
    fi
    
    echo ""

    echo "🔐 IMPORTANT: Keep credentials secure!"
    echo "     File saved to: .env/cockroachdb_cdc_azure.json (should be in .gitignore)"
    echo ""
    
    echo "📋 Available URLs (saved in JSON):"
    echo "   CockroachDB Changefeed URI:"
    echo "     azure-blob://${credentials[azure_storage_container]}?AZURE_ACCOUNT_NAME=..."
    echo ""
    echo "   ADLS Gen2 (abfss://) - For Databricks with hierarchical namespace:"
    echo "     Parquet: ${credentials[abfss_parquet_url]}"
    echo "     JSON:    ${credentials[abfss_json_url]}"
    echo ""
    echo "   Blob Storage (wasbs://) - For standard Azure Blob Storage:"
    echo "     Parquet: ${credentials[wasbs_parquet_url]}"
    echo "     JSON:    ${credentials[wasbs_json_url]}"
    echo ""
    
    # Display managed identity info if configured
    if [ -n "${credentials[managed_identity_type]:-}" ]; then
        echo "🔐 Managed Identity (saved in JSON):"
        echo "   Type: ${credentials[managed_identity_type]}"
        if [ -n "${credentials[managed_identity_client_id]:-}" ]; then
            echo "   Client ID: ${credentials[managed_identity_client_id]}"
        fi
        if [ -n "${credentials[access_connector_id]:-}" ]; then
            echo "   Access Connector ID: ${credentials[access_connector_id]}"
        fi
        if [ -n "${credentials[managed_identity_resource_id]:-}" ]; then
            echo "   Resource ID: ${credentials[managed_identity_resource_id]}"
        fi
        if [ -n "${credentials[managed_identity_principal_id]:-}" ]; then
            echo "   Principal ID: ${credentials[managed_identity_principal_id]}"
        fi
        echo ""
        echo "   💡 To use managed identity in Databricks:"
        echo "      • Configure your Databricks cluster with this managed identity"
        echo "      • No need to pass storage account keys"
        echo "      • Authentication is automatic"
        echo ""
    fi
else
    echo "✅ Using existing credentials from: $JSON_FILE"
    echo ""
fi

echo ""
echo "🧪 To verify Azure Blob Storage:"
if [ -n "${credentials[azure_storage_key]}" ]; then
    echo "   az storage blob list \\"
    echo "     --account-name ${credentials[azure_storage_account]} \\"
    echo "     --account-key ${credentials[azure_storage_key]} \\"
    echo "     --container-name ${credentials[azure_storage_container]} \\"
    echo "     --output table"
else
    echo "   az storage blob list \\"
    echo "     --account-name ${credentials[azure_storage_account]} \\"
    echo "     --account-key <YOUR_KEY> \\"
    echo "     --container-name ${credentials[azure_storage_container]} \\"
    echo "     --output table"
fi
echo ""

if [ "$SKIP_JSON_SAVE" = true ]; then
    echo "✅ Using existing Azure Blob Storage configuration!"
    echo "   Credentials loaded from: $JSON_FILE"
elif [ "$SKIP_CREATION" = true ]; then
    echo "✅ Connected to existing Azure resources!"
    echo "   Credentials saved to: $JSON_FILE"
else
    echo "✅ Azure Blob Storage setup complete!"
    echo "   Credentials saved to: $JSON_FILE"
fi

# Display managed identity usage instructions if configured
if [ -n "${credentials[managed_identity_type]:-}" ]; then
    echo ""
    echo "================================================================"
    echo "🔐 MANAGED IDENTITY USAGE"
    echo "================================================================"
    echo ""
    echo "Your storage account is configured with managed identity authentication."
    echo ""
    echo "📋 To use with Databricks:"
    echo ""
    echo "1. **Assign managed identity to your Databricks cluster:**"
    echo "   • Go to Compute → Select your cluster → Edit"
    echo "   • Under 'Advanced Options' → 'Instances'"
    echo "   • Add the managed identity to the cluster"
    echo ""
    echo "2. **Use in notebooks (no keys needed):**
    echo ""
    echo "   # Python example"
    echo "   df = spark.read.parquet("
    echo "       \"${credentials[abfss_parquet_url]}\""
    echo "   )"
    echo ""
    echo "3. **For Unity Catalog External Locations:**"
    echo ""
    echo "   CREATE STORAGE CREDENTIAL cockroachdb_cdc_credential"
    echo "   WITH ("
    echo "     AZURE_MANAGED_IDENTITY ("
    if [ "${credentials[managed_identity_type]:-}" = "user_assigned" ]; then
        echo "       MANAGED_IDENTITY_ID '${credentials[managed_identity_client_id]}'"
    else
        echo "       -- System-assigned managed identity"
    fi
    echo "     )"
    echo "   );"
    echo ""
    echo "   CREATE EXTERNAL LOCATION cockroachdb_cdc_parquet"
    echo "   URL '${credentials[abfss_parquet_url]}'"
    echo "   WITH (STORAGE CREDENTIAL cockroachdb_cdc_credential);"
    echo ""
    echo "💡 Benefits of managed identity:"
    echo "   ✅ No keys to manage or rotate"
    echo "   ✅ Automatic credential refresh"
    echo "   ✅ Fine-grained RBAC permissions"
    echo "   ✅ Audit logging of access"
    echo ""
    echo "================================================================"
    echo ""
fi