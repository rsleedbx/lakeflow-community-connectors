# Environment Variable and Token Format Guide

This document describes the expected format for environment variables and credential tokens used by the CockroachDB CDC connector.

## Azure Storage Configuration

### Environment Variables

```bash
export AZURE_STORAGE_ACCOUNT=your-storage-account-name
export AZURE_STORAGE_KEY=your-azure-storage-key-here
export AZURE_STORAGE_CONTAINER=changefeed-events
```

### JSON Configuration File

Location: `sources/cockroachdb/.env/cockroachdb_cdc_azure.json`

```json
{
  "azure_storage_account": "your-storage-account-name",
  "azure_storage_key": "your-azure-storage-key-here",
  "azure_storage_container": "changefeed-events",
  "changefeed_uri": "azure-blob://changefeed-events?AZURE_ACCOUNT_NAME=your-account&AZURE_ACCOUNT_KEY=your-key"
}
```

## CockroachDB Connection Configuration

### Full URL Format

```bash
export COCKROACHDB_URL="postgresql://username:password@your-cluster.cockroachlabs.cloud:26257/database?sslmode=require"
```

### Token + Base URL Format

```bash
export COCKROACHDB_TOKEN="username:password"
export COCKROACHDB_BASE_URL="postgresql://your-cluster.cockroachlabs.cloud:26257/database?sslmode=require"
```

### JSON Configuration File

Location: `sources/cockroachdb/.env/cockroachdb_credentials.json`

```json
{
  "cockroachdb_url": "postgresql://username:password@your-cluster.cockroachlabs.cloud:26257/database?sslmode=require"
}
```

## Complete .env File Structure

```bash
# Azure Blob Storage for CockroachDB Changefeed
export AZURE_STORAGE_ACCOUNT=your-storage-account-name
export AZURE_STORAGE_KEY=your-azure-storage-key-here
export AZURE_STORAGE_CONTAINER=changefeed-events
export AZURE_CONNECTION_STRING=DefaultEndpointsProtocol=https;AccountName=your-account;AccountKey=your-key;EndpointSuffix=core.windows.net

# CockroachDB Connection (Full URL format)
export COCKROACHDB_URL="postgresql://username:password@your-cluster.cockroachlabs.cloud:26257/database?sslmode=require"

# CockroachDB Connection (GitHub-style token + base_url format - used by cockroachdb.py)
export COCKROACHDB_TOKEN="username:password"
export COCKROACHDB_BASE_URL="postgresql://your-cluster.cockroachlabs.cloud:26257/database?sslmode=require"
```

## Setting Up Credentials

### Option 1: Use JSON Config Files (Recommended)

1. Copy templates:
```bash
cd sources/cockroachdb/.env
cp cockroachdb_cdc_azure.json.template cockroachdb_cdc_azure.json
```

2. Edit the JSON files with your actual credentials

3. Files are automatically gitignored

### Option 2: Use Environment Variables

Set the variables in your shell:

```bash
export AZURE_STORAGE_ACCOUNT=your-account
export AZURE_STORAGE_KEY=your-key
export COCKROACHDB_URL=your-connection-string
```

### Option 3: Run Setup Script

```bash
cd sources/cockroachdb/scripts
./01_azure_storage.sh
```

This will automatically:
- Create Azure storage resources
- Generate configuration files
- Set up credentials

## Token Format Examples

### Azure Storage Key
- **Format:** Base64-encoded string, typically 88 characters
- **Example:** `your-base64-encoded-azure-key-here==`

### CockroachDB URL
- **Format:** PostgreSQL connection string
- **Example:** `postgresql://username:password@cluster.cockroachlabs.cloud:26257/dbname?sslmode=require`

### CockroachDB Token
- **Format:** `username:password`
- **Example:** `myuser:mypassword`

## Security Best Practices

1. **Never commit credentials to git**
   - All `*.json` files in `.env/` directories are gitignored
   - Use templates (`.template` files) for documentation

2. **Use Databricks Secrets for production**
   ```python
   dbutils.secrets.get(scope="cockroachdb_cdc", key="azure_storage_key")
   ```

3. **Rotate credentials regularly**
   - Azure storage keys should be rotated every 90 days
   - CockroachDB passwords should be rotated every 90 days

4. **Use least privilege**
   - Azure: Only grant necessary permissions (read/write to specific container)
   - CockroachDB: Create dedicated user with minimal required privileges

## Troubleshooting

### Invalid Azure Key
```
Error: InvalidAuthenticationInfo
```
- Verify the key hasn't been rotated
- Check for extra whitespace in the key
- Ensure the key is base64-encoded

### Invalid CockroachDB URL
```
Error: connection refused
```
- Verify the cluster hostname
- Check the port (default: 26257)
- Ensure `sslmode=require` is included
- Verify username/password are correct

### Missing Configuration Files
```
Error: Missing cockroachdb_cdc_azure.json
```
- Run `./01_azure_storage.sh` to generate configuration
- Or copy from template: `cp cockroachdb_cdc_azure.json.template cockroachdb_cdc_azure.json`

## Notes

- This connector was tested with CockroachDB v23.1+ and Azure Blob Storage
- The JSON configuration format is the recommended approach
- Environment variables are supported but JSON configs are preferred for automation
- All credential files in `.env/` directories are automatically gitignored

## See Also

- [Azure Storage Setup](../scripts/01_azure_storage.sh)
- [Test CDC Matrix](../scripts/test_cdc_matrix.sh)
- [Connector Evolution Strategy](../CONNECTOR_EVOLUTION_STRATEGY.md)
