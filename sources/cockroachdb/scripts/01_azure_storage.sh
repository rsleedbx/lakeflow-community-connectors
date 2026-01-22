#!/usr/bin/env bash

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

# Default configuration
RG_NAME="cockroachdb-cdc-rg"
CONTAINER_NAME="changefeed-events"

# #############################################################################
# AZ Cloud

AZ_INIT

# read or default credentials
declare -A credentials
if [ -f "$JSON_FILE" ]; then
    json_to_associative_array credentials "$JSON_FILE"
fi

# Single timestamp for consistent resource naming (all resources in this run share same suffix)
credentials[timestamp]=${credentials[timestamp]:-$(date +%s)}

credentials[resource_group]=${credentials[resource_group]:-$RG_NAME}
credentials[azure_storage_account]=${credentials[azure_storage_account]:-cockroachcdc${credentials[timestamp]}}
credentials[azure_storage_container]="${credentials[azure_storage_container]:-$CONTAINER_NAME}"

# create resource group
if ! AZ group show --resource-group "${RG_NAME}" ; then
    # multiples tags are defined correctly below.  NOT A MISTAKE
    DB_EXIT_ON_ERROR="PRINT_EXIT" AZ group create --resource-group "${RG_NAME}" \
        --tags "Owner=${DBX_USERNAME}" "${REMOVE_AFTER:+RemoveAfter=${REMOVE_AFTER}}"
fi


# get/create the storage
if ! AZ storage account show --name "${credentials[azure_storage_account]}" --resource-group "${credentials[resource_group]}"; then
    DB_EXIT_ON_ERROR="PRINT_EXIT" AZ storage account create \
      --name "${credentials[azure_storage_account]}" \
      --resource-group "${credentials[resource_group]}" \
      --sku Standard_LRS \
      --kind StorageV2 \
      --access-tier Hot \
      --allow-blob-public-access false \
      --enable-hierarchical-namespace true \
      --location "${CLOUD_LOCATION}"
    credentials[azure_storage_key]=''   # remove previous key if any
    associative_array_to_json_file credentials "$JSON_FILE"
fi

# get storage key
if [[ -z "${credentials[azure_storage_key]:-}" ]]; then 
    DB_EXIT_ON_ERROR="PRINT_EXIT" AZ storage account keys list \
      --resource-group "${credentials[resource_group]}" \
      --account-name "${credentials[azure_storage_account]}"
    credentials[azure_storage_key]=$(jq -r '.[0].value' /tmp/az_stdout.$$)
    associative_array_to_json_file credentials "$JSON_FILE"
fi

# get/create container
if ! AZ  storage container show --name "${credentials[azure_storage_container]}" --account-name "${credentials[azure_storage_account]}" \
      --account-key "${credentials[azure_storage_key]}"; then
    DB_EXIT_ON_ERROR="PRINT_EXIT" AZ storage container create \
      --name "${credentials[azure_storage_container]}" \
      --account-name "${credentials[azure_storage_account]}" \
      --account-key "${credentials[azure_storage_key]}"
fi

# build URLs
credentials[changefeed_uri]="azure-blob://${credentials[azure_storage_container]}?AZURE_ACCOUNT_NAME=${credentials[azure_storage_account]}&AZURE_ACCOUNT_KEY=${credentials[azure_storage_key]}"
credentials[abfss_base_url]="abfss://${credentials[azure_storage_container]}@${credentials[azure_storage_account]}.dfs.core.windows.net"    # required by Databricks UI
credentials[abfss_parquet_url]="${credentials[abfss_base_url]}/parquet-cdc"
credentials[abfss_json_url]="${credentials[abfss_base_url]}/json-cdc"
credentials[wasbs_base_url]="wasbs://${credentials[azure_storage_container]}@${credentials[azure_storage_account]}.blob.core.windows.net"
credentials[wasbs_parquet_url]="${credentials[wasbs_base_url]}/parquet-cdc"
credentials[wasbs_json_url]="${credentials[wasbs_base_url]}/json-cdc"

# save to JSON
associative_array_to_json_file credentials "$JSON_FILE"

# #############################################################################
# create manage identity 

echo -e "\n\b Need IAM permission on storage account \n\n"

# managed identity (user-assigned only)
credentials[managed_identity_name]=${credentials[managed_identity_name]:-cockroachdb-cdc-identity-${credentials[timestamp]}}
if ! AZ identity show --name "${credentials[managed_identity_name]}" --resource-group "${credentials[resource_group]}"; then 
    DB_EXIT_ON_ERROR="PRINT_EXIT" AZ identity create \
        --name "${credentials[managed_identity_name]}" \
        --resource-group "${credentials[resource_group]}"
    # load identity details from either show or create
    credentials[managed_identity_type]="user_assigned"
    credentials[managed_identity_client_id]=$(jq -r '.clientId' /tmp/az_stdout.$$)
    credentials[managed_identity_resource_id]=$(jq -r '.id' /tmp/az_stdout.$$)          # required by Databricks UI
    credentials[managed_identity_principal_id]=$(jq -r '.principalId' /tmp/az_stdout.$$)
    associative_array_to_json_file credentials "$JSON_FILE"
fi

# get storage resource ID for RBAC
if [[ -z "${credentials[storage_resource_id]:-}" ]]; then
    DB_EXIT_ON_ERROR="PRINT_EXIT" AZ storage account show \
        --name "${credentials[azure_storage_account]}" \
        --resource-group "${credentials[resource_group]}"
    credentials[storage_resource_id]=$(jq -r '.id' /tmp/az_stdout.$$)
    associative_array_to_json_file credentials "$JSON_FILE"
fi

# #############################################################################
# Optional: Assign RBAC roles to managed identity for enhanced Databricks functionality
# 
# These role assignments enable:
# - Automatic file event subscriptions (recommended for Autoloader)
# - Databricks-managed EventGrid configuration
# - Future Databricks features that rely on event notifications
#
# Requirements:
# - Owner or User Access Administrator role on the storage account
# - Owner or User Access Administrator role on the resource group
#
# If you lack these permissions, you can:
# - Configure file events manually later
# - Use the access connector without these roles (basic functionality works)
# - Ask your Azure admin to run this function
# #############################################################################

assign_managed_identity_roles() {
    # Step 2: Read and write access to storage blobs
    DB_EXIT_ON_ERROR="PRINT_EXIT" AZ role assignment create \
        --role "Storage Blob Data Contributor" \
        --assignee "${credentials[managed_identity_principal_id]}" \
        --scope "${credentials[storage_resource_id]}"
    
    # Step 3: Access to file events (queue notifications)
    DB_EXIT_ON_ERROR="PRINT_EXIT" AZ role assignment create \
        --role "Storage Queue Data Contributor" \
        --assignee "${credentials[managed_identity_principal_id]}" \
        --scope "${credentials[storage_resource_id]}"
    
    # Step 4a: Allow Databricks to configure file events (storage account level)
    DB_EXIT_ON_ERROR="PRINT_EXIT" AZ role assignment create \
        --role "Storage Account Contributor" \
        --assignee "${credentials[managed_identity_principal_id]}" \
        --scope "${credentials[storage_resource_id]}"
    
    # Step 4b: Allow Databricks to configure file events (resource group level for EventGrid)
    # Get resource group ID if not already set
    if [[ -z "${credentials[resource_group_id]:-}" ]]; then
        DB_EXIT_ON_ERROR="PRINT_EXIT" AZ group show \
            --resource-group "${credentials[resource_group]}"
        credentials[resource_group_id]=$(jq -r '.id' /tmp/az_stdout.$$)
    fi
    
    DB_EXIT_ON_ERROR="PRINT_EXIT" AZ role assignment create \
        --role "EventGrid EventSubscription Contributor" \
        --assignee "${credentials[managed_identity_principal_id]}" \
        --scope "${credentials[resource_group_id]}"
}

# Skip RBAC role assignments by default (uncomment to enable)
# Requires Owner or User Access Administrator permissions
# assign_managed_identity_roles

# create access connector for Databricks Unity Catalog (ignore error if permission denied)
credentials[access_connector_name]=${credentials[access_connector_name]:-cockroachdb-cdc-access-connector-${credentials[timestamp]}}
if ! AZ databricks access-connector show \
    --name "${credentials[access_connector_name]}" \
    --resource-group "${credentials[resource_group]}"; then
    
     DB_EXIT_ON_ERROR="PRINT_EXIT" AZ databricks access-connector create \
        --name "${credentials[access_connector_name]}" \
        --resource-group "${credentials[resource_group]}" \
        --location "${CLOUD_LOCATION}" \
        --identity-type UserAssigned \
        --user-assigned-identities "{\"${credentials[managed_identity_resource_id]}\": {}}"
    credentials[access_connector_id]=$(jq -r '.id // empty' /tmp/az_stdout.$$)      # required by Databricks UI
    associative_array_to_json_file credentials "$JSON_FILE"
fi

# load access connector ID from either show or create
if [[ -z "${credentials[access_connector_id]:-}" ]]; then
    echo "setting access_connector_id ${credentials[access_connector_id]:-}"

    credentials[access_connector_id]=$(jq -r '.id // empty' /tmp/az_stdout.$$)
    associative_array_to_json_file credentials "$JSON_FILE"
fi

# Validate access connector ID was retrieved
if [[ -z "${credentials[access_connector_id]}" ]]; then
    echo "⚠️  Warning: Could not retrieve Access Connector ID"
    echo "   Unity Catalog setup may require manual configuration"
fi

# #############################################################################
# Unity Catalog Setup
#
# This creates:
# - Unity Catalog storage credential (using managed identity or access key)
# - External locations for Parquet and JSON CDC data
# - Permissions for the current Databricks user
#
# Resource names include timestamp suffix for association with Azure resources
#
# Requirements:
# - Databricks CLI installed and configured
# - Unity Catalog enabled in your workspace
# - Account admin or metastore admin privileges
#
# If you lack these requirements:
# - Create storage credential manually via Databricks UI
# - Use the notebook with embedded credentials
# #############################################################################

# Initialize Databricks CLI and get current user
DBX_INIT

# Configuration - Databricks UI uses concatenated 
# External Location Name + _ + azuremanagedidendity_$(date +%s%6)
# Connector Id: /subscriptions/xxx/resourceGroups/cockroachdb-cdc-rg/providers/Microsoft.Databricks/accessConnectors/cockroachdb-cdc-access-connector-1768934658
# User Assigned Managed Identity Id: /subscriptions/xxx/resourcegroups/cockroachdb-cdc-rg/providers/Microsoft.ManagedIdentity/userAssignedIdentities/cockroachdb-cdc-identity-1768934658

if [[ -z "${credentials[unity_catalog__storage_credential_name]:-}" ]]; then
    credentials[unity_catalog__storage_credential_name]="${credentials[azure_storage_account]}"
fi

if ! DBX storage-credentials get "${credentials[unity_catalog__storage_credential_name]}"; then    
    storage_cred_json=$(cat <<EOF
{
  "name": "${credentials[unity_catalog__storage_credential_name]}",
  "comment": "CockroachDB CDC changefeeds",
  "azure_managed_identity": {
    "access_connector_id": "${credentials[access_connector_id]}",
    "managed_identity_id": "${credentials[managed_identity_resource_id]}"
  },
  "read_only": false,
  "skip_validation": false
}
EOF
)
    DB_EXIT_ON_ERROR="PRINT_EXIT" DBX storage-credentials create --json "$storage_cred_json"
    associative_array_to_json_file credentials "$JSON_FILE"
fi

# fallback if json file is missing the id\
if [[ -z "${credentials[unity_catalog__storage_credential_id]:-}" ]]; then
    echo "setting storage_credential_id ${credentials[unity_catalog__storage_credential_id]:-}"
    credentials[unity_catalog__storage_credential_id]=$(jq -r '.id // empty' /tmp/dbx_stdout.$$)
    associative_array_to_json_file credentials "$JSON_FILE"
fi

# create external location if not exists

if [[ -z "${credentials[unity_catalog__external_location]:-}" ]]; then
    credentials[unity_catalog__external_location]="${credentials[azure_storage_account]}"
fi

if ! DBX external-locations get "${credentials[unity_catalog__external_location]}"; then
    DB_EXIT_ON_ERROR="PRINT_EXIT" DBX external-locations create \
        "${credentials[unity_catalog__external_location]}" \
        "${credentials[abfss_base_url]}" \
        "${credentials[unity_catalog__storage_credential_name]}" \
        --comment "CockroachDB CDC" 
    # Save all credentials to JSON
    associative_array_to_json_file credentials "$JSON_FILE"
fi
