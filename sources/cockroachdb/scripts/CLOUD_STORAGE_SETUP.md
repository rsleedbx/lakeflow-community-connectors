# Cloud Storage Setup for CockroachDB CDC

This directory contains automated setup scripts for Azure and AWS cloud storage with Unity Catalog integration.

## Overview

Both scripts follow the same pattern:
1. Create cloud storage resources (Azure Storage Account + Container OR S3 Bucket)
2. Set up authentication (Azure Managed Identity + Access Connector OR AWS IAM Role)
3. Configure Databricks Unity Catalog (Storage Credential + External Locations)
4. Save all credentials to JSON for use by notebooks and other scripts

## Azure Setup (`01_azure_storage.sh`)

### Prerequisites
- Azure CLI installed and configured (`az login`)
- Databricks CLI installed and configured
- Unity Catalog enabled in Databricks workspace

### What it creates
- **Azure Storage Account** (ADLS Gen2 with hierarchical namespace)
- **Container**: `changefeed-events`
- **User-Assigned Managed Identity**: For keyless authentication
- **Azure Databricks Access Connector**: Bridge between Databricks and Azure storage
- **Unity Catalog Resources**:
  - Storage Credential (using managed identity)
  - External Locations for Parquet and JSON data
  - READ_FILES permissions

### Usage
```bash
cd sources/cockroachdb/scripts
source 00_lakeflow_connect_env.sh
./01_azure_storage.sh
```

### Output
- Credentials saved to: `.env/cockroachdb_cdc_azure.json`
- Changefeed URI format: `azure-blob://container?AZURE_ACCOUNT_NAME=...&AZURE_ACCOUNT_KEY=...`
- Databricks access via: `abfss://container@account.dfs.core.windows.net/path`

### Key Azure Concepts
- **ADLS Gen2**: Azure Data Lake Storage Gen2 (hierarchical namespace enabled)
- **`abfss://` protocol**: Azure Blob File System Secure (for Databricks/Spark)
- **`wasbs://` protocol**: Windows Azure Storage Blob Secure (legacy, still supported)
- **Managed Identity**: Keyless authentication (recommended over access keys)
- **Access Connector**: Required for Unity Catalog with managed identities

### Optional: RBAC Role Assignments
Uncomment `assign_managed_identity_roles` in the script to enable:
- Storage Blob Data Contributor
- Storage Queue Data Contributor
- Storage Account Contributor
- EventGrid EventSubscription Contributor

These roles enable advanced features like automatic file event subscriptions for Databricks Autoloader.

**Requirements**: Owner or User Access Administrator role on the storage account and resource group.

## AWS Setup (`01_aws_storage.sh`)

### Prerequisites
- AWS CLI installed and configured (`aws configure` or `aws sso login`)
- Databricks CLI installed and configured
- Unity Catalog enabled in Databricks workspace

### What it creates
- **S3 Bucket** (with versioning and public access blocking)
- **IAM Role**: For Databricks to assume
- **IAM Policy**: S3 access permissions attached to the role
- **Unity Catalog Resources**:
  - Storage Credential (using IAM role)
  - External Locations for Parquet and JSON data
  - READ_FILES permissions

### Usage
```bash
cd sources/cockroachdb/scripts
source 00_lakeflow_connect_env.sh

# Set Databricks account details (required for IAM trust policy)
export DATABRICKS_ACCOUNT_ID="414351767826"  # Default Databricks AWS account
export DATABRICKS_EXTERNAL_ID="your-external-id"  # From Databricks Unity Catalog setup

./01_aws_storage.sh
```

### Output
- Credentials saved to: `.env/cockroachdb_cdc_aws.json`
- Changefeed URI format: `s3://bucket?AWS_ACCESS_KEY_ID=...&AWS_SECRET_ACCESS_KEY=...&AWS_REGION=...`
- Databricks access via: `s3://bucket/path`

### Key AWS Concepts
- **S3**: Simple Storage Service (object storage)
- **IAM Role**: Cross-account access using AssumeRole
- **External ID**: Security feature to prevent confused deputy problem
- **Trust Policy**: Defines who can assume the IAM role
- **IAM Policy**: Defines what the role can do

### IAM Role Trust Policy
The IAM role is configured to trust the Databricks AWS account. You'll need to:
1. Get your Databricks account ID (usually `414351767826` for commercial cloud)
2. Get the External ID from Databricks Unity Catalog UI when creating storage credential
3. Set these as environment variables before running the script

## Comparison: Azure vs AWS

| Feature | Azure | AWS |
|---------|-------|-----|
| **Storage** | Storage Account + Container | S3 Bucket |
| **Authentication** | Managed Identity + Access Connector | IAM Role |
| **URL Format** | `abfss://container@account.dfs.core.windows.net` | `s3://bucket` |
| **Changefeed URI** | `azure-blob://` | `s3://` |
| **Access Control** | RBAC roles | IAM policies |
| **Unity Catalog Bridge** | Access Connector (required) | IAM role trust policy |
| **Keyless Auth** | Managed Identity (native) | IAM Role AssumeRole |

## Common Patterns

Both scripts follow these patterns:
- **Idempotent**: Safe to run multiple times
- **Associative Arrays**: Python-style dictionaries in bash (`credentials[key]`)
- **JSON Persistence**: All credentials saved to `.env/*.json`
- **Timestamp Suffixed Resources**: All resources share same timestamp for easy association
- **Wrapper Functions**: `AZ` and `AWS` wrappers with consistent error handling
- **Unity Catalog Integration**: Automated storage credential and external location setup

## Troubleshooting

### Azure: "Access Connector could not be found"
- **Cause**: Databricks workspace can't access the Access Connector
- **Fix**: Grant Reader role on the Access Connector to the Databricks workspace's managed identity
- **Alternative**: Manually create the storage credential in Databricks UI

### AWS: "Invalid IAM role"
- **Cause**: IAM role trust policy doesn't allow Databricks to assume it
- **Fix**: Ensure `DATABRICKS_ACCOUNT_ID` and `DATABRICKS_EXTERNAL_ID` are correctly set
- **Alternative**: Manually create the storage credential in Databricks UI

### Unity Catalog: "No such storage credential"
- **Cause**: Storage credential creation failed
- **Fix**: Check Databricks CLI configuration and Unity Catalog permissions
- **Requirement**: Account admin or metastore admin privileges

## Next Steps

After running the setup script:

1. **Test connectivity**:
   ```bash
   # Azure
   az storage blob list --account-name <account> --container-name changefeed-events
   
   # AWS
   aws s3 ls s3://<bucket>/
   ```

2. **Create CockroachDB changefeed**:
   ```sql
   -- Azure
   CREATE CHANGEFEED FOR TABLE mytable 
   INTO 'azure-blob://...' 
   WITH format='parquet';
   
   -- AWS
   CREATE CHANGEFEED FOR TABLE mytable 
   INTO 's3://...' 
   WITH format='parquet';
   ```

3. **Access from Databricks notebook**:
   ```python
   # Load from Unity Catalog external location
   df = spark.read.format("parquet").load("s3://bucket/parquet-cdc/")
   # or
   df = spark.read.format("parquet").load("abfss://container@account.dfs.core.windows.net/parquet-cdc/")
   ```

## Related Scripts

- `test_azure_cdc.sh`: Test Azure CDC changefeeds
- `setup_azure_blob_for_cdc.sh`: Legacy Azure setup (being deprecated)
- `setup_unity_catalog.sh`: Standalone Unity Catalog setup (now integrated)

## References

- [CockroachDB CDC Documentation](https://www.cockroachlabs.com/docs/stable/change-data-capture-overview.html)
- [Databricks Unity Catalog](https://docs.databricks.com/data-governance/unity-catalog/index.html)
- [Azure ADLS Gen2](https://docs.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction)
- [AWS S3](https://docs.aws.amazon.com/s3/)





