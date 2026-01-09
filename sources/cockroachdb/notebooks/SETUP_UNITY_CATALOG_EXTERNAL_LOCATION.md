# Setup Unity Catalog External Location for CockroachDB CDC

**Purpose**: Configure secure access to Azure Blob Storage using Unity Catalog External Locations instead of embedding credentials in notebooks.

**Benefits**:
- ✅ No credentials in notebook code
- ✅ Centralized access control
- ✅ Audit trail of data access
- ✅ Share access across multiple notebooks/users
- ✅ Automatic credential rotation

---

## Prerequisites

- Databricks workspace with Unity Catalog enabled
- Account admin or metastore admin privileges (for initial setup)
- Azure Blob Storage account with CockroachDB CDC files
- Azure credentials from `cockroachdb_cdc_azure.json`

---

## Step 1: Create Storage Credential (One-time Setup)

### Option A: Using Databricks UI

1. **Navigate to Storage Credentials**:
   - Go to **Catalog** → **External Data** → **Storage Credentials**
   - Click **Create Credential**

2. **Configure Storage Credential**:
   ```
   Name: cockroachdb-cdc-storage-credential
   Type: Azure Blob Storage
   Authentication Type: Storage Account Key
   Storage Account Name: cockroachcdc1766504458
   Storage Account Key: <paste from cockroachdb_cdc_azure.json>
   ```

3. **Set Owner**: Your user or group

4. **Click Create**

### Option B: Using SQL

```sql
-- Create storage credential with Azure storage account key
CREATE STORAGE CREDENTIAL cockroachdb_cdc_storage_credential
WITH (
  AZURE_STORAGE_ACCOUNT_NAME 'cockroachcdc1766504458',
  AZURE_STORAGE_ACCOUNT_KEY '<paste-your-key-here>'
);

-- Grant permissions to users/groups who need access
GRANT CREATE EXTERNAL LOCATION ON STORAGE CREDENTIAL cockroachdb_cdc_storage_credential TO <user_or_group>;
```

**Get your storage account key**:
```bash
cd /Users/robert.lee/github/lakeflow-community-connectors/sources/cockroachdb/.env
cat cockroachdb_cdc_azure.json | jq -r '.azure_storage_key'
```

---

## Step 2: Create External Location

### Option A: Using Databricks UI

1. **Navigate to External Locations**:
   - Go to **Catalog** → **External Data** → **External Locations**
   - Click **Create Location**

2. **Configure External Location**:
   ```
   Name: cockroachdb-cdc-parquet
   URL: wasbs://changefeed-events@cockroachcdc1766504458.blob.core.windows.net/parquet-cdc
   Storage Credential: cockroachdb-cdc-storage-credential
   Comment: CockroachDB CDC Parquet files from Azure Blob Storage
   ```

3. **Set Owner**: Your user or group

4. **Click Create**

5. **Test Connection**: Click "Test connection" to verify access

### Option B: Using SQL

```sql
-- Create external location for Parquet CDC files
CREATE EXTERNAL LOCATION cockroachdb_cdc_parquet
URL 'wasbs://changefeed-events@cockroachcdc1766504458.blob.core.windows.net/parquet-cdc'
WITH (
  STORAGE CREDENTIAL cockroachdb_cdc_storage_credential
);

-- Optional: Create separate location for JSON CDC files
CREATE EXTERNAL LOCATION cockroachdb_cdc_json
URL 'wasbs://changefeed-events@cockroachcdc1766504458.blob.core.windows.net/json-cdc'
WITH (
  STORAGE CREDENTIAL cockroachdb_cdc_storage_credential
);

-- Grant permissions to users/groups
GRANT READ FILES ON EXTERNAL LOCATION cockroachdb_cdc_parquet TO <user_or_group>;
GRANT READ FILES ON EXTERNAL LOCATION cockroachdb_cdc_json TO <user_or_group>;
```

---

## Step 3: Grant Permissions

### Grant to specific users:
```sql
-- Grant read access to specific user
GRANT READ FILES ON EXTERNAL LOCATION cockroachdb_cdc_parquet TO `user@company.com`;

-- Grant to multiple users
GRANT READ FILES ON EXTERNAL LOCATION cockroachdb_cdc_parquet TO `user1@company.com`;
GRANT READ FILES ON EXTERNAL LOCATION cockroachdb_cdc_parquet TO `user2@company.com`;
```

### Grant to groups:
```sql
-- Grant read access to a group
GRANT READ FILES ON EXTERNAL LOCATION cockroachdb_cdc_parquet TO `data-engineers`;
GRANT READ FILES ON EXTERNAL LOCATION cockroachdb_cdc_parquet TO `analysts`;
```

### Check permissions:
```sql
-- Show grants for external location
SHOW GRANTS ON EXTERNAL LOCATION cockroachdb_cdc_parquet;
```

---

## Step 4: Update Notebook to Use External Location

Once the external location is set up, update your `load_parquet_files.ipynb` notebook:

### Replace Configuration Cell:

**OLD (with embedded credentials)**:
```python
# Azure Storage Configuration
AZURE_STORAGE_ACCOUNT = azure_config["azure_storage_account"]
AZURE_STORAGE_KEY = azure_config["azure_storage_key"]
AZURE_CONTAINER = azure_config.get("azure_storage_container", "changefeed-events")
PATH_PREFIX = "parquet-cdc"

# Construct path
AZURE_SOURCE_PATH = f"wasbs://{AZURE_CONTAINER}@{AZURE_STORAGE_ACCOUNT}.blob.core.windows.net/{PATH_PREFIX}/"
```

**NEW (with external location - no credentials needed!)**:
```python
# Use Unity Catalog External Location (no credentials needed!)
USE_EXTERNAL_LOCATION = True

if USE_EXTERNAL_LOCATION:
    # No credentials needed - permissions managed by Unity Catalog
    EXTERNAL_LOCATION_NAME = "cockroachdb_cdc_parquet"
    AZURE_SOURCE_PATH = f"wasbs://changefeed-events@cockroachcdc1766504458.blob.core.windows.net/parquet-cdc/"
    print(f"✅ Using Unity Catalog External Location: {EXTERNAL_LOCATION_NAME}")
    print(f"🔐 Authentication: Managed by Unity Catalog (no credentials in code)")
else:
    # Fallback to credential-based authentication
    AZURE_STORAGE_ACCOUNT = azure_config["azure_storage_account"]
    AZURE_STORAGE_KEY = azure_config["azure_storage_key"]
    AZURE_CONTAINER = azure_config.get("azure_storage_container", "changefeed-events")
    PATH_PREFIX = "parquet-cdc"
    AZURE_SOURCE_PATH = f"wasbs://{AZURE_CONTAINER}@{AZURE_STORAGE_ACCOUNT}.blob.core.windows.net/{PATH_PREFIX}/"
```

### Remove Authentication Cell:

**DELETE (no longer needed)**:
```python
# ❌ This entire cell is not needed with External Locations:
spark.conf.set(...)  # Not needed
.option("fs.azure.account.key...", AZURE_STORAGE_KEY)  # Not needed
```

### Simplified Autoloader:

**OLD**:
```python
df_raw = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option(f"fs.azure.account.key.{AZURE_STORAGE_ACCOUNT}.blob.core.windows.net", AZURE_STORAGE_KEY)  # ❌ Not needed
    .load(AZURE_SOURCE_PATH)
)
```

**NEW**:
```python
df_raw = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    # No credential options needed! Unity Catalog handles authentication
    .load(AZURE_SOURCE_PATH)
)
```

---

## Step 5: Test Access

### Test 1: List files using external location
```python
# Test that you can access the external location
files = dbutils.fs.ls("wasbs://changefeed-events@cockroachcdc1766504458.blob.core.windows.net/parquet-cdc/")
print(f"✅ Found {len(files)} objects")
for f in files[:5]:
    print(f"  - {f.name}")
```

### Test 2: Read a sample file
```python
# Test reading a Parquet file
sample_df = spark.read.parquet("wasbs://changefeed-events@cockroachcdc1766504458.blob.core.windows.net/parquet-cdc/")
print(f"✅ Successfully read {sample_df.count()} rows")
sample_df.printSchema()
```

### Test 3: Run Autoloader
```python
# Test Autoloader with external location
test_stream = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", "/tmp/test_schema")
    .load("wasbs://changefeed-events@cockroachcdc1766504458.blob.core.windows.net/parquet-cdc/")
)

print("✅ Autoloader stream configured successfully")
print(f"Schema: {test_stream.schema}")
```

---

## Troubleshooting

### Error: "PERMISSION_DENIED"
**Cause**: User doesn't have access to external location

**Solution**:
```sql
-- Grant read access
GRANT READ FILES ON EXTERNAL LOCATION cockroachdb_cdc_parquet TO `user@company.com`;

-- Or grant to everyone in workspace (use with caution)
GRANT READ FILES ON EXTERNAL LOCATION cockroachdb_cdc_parquet TO `account users`;
```

### Error: "EXTERNAL_LOCATION_NOT_FOUND"
**Cause**: External location doesn't exist or name is wrong

**Solution**:
```sql
-- List all external locations
SHOW EXTERNAL LOCATIONS;

-- Check if yours exists
DESCRIBE EXTERNAL LOCATION cockroachdb_cdc_parquet;
```

### Error: "STORAGE_CREDENTIAL_NOT_FOUND"
**Cause**: Storage credential doesn't exist or is not accessible

**Solution**:
```sql
-- List all storage credentials
SHOW STORAGE CREDENTIALS;

-- Check if yours exists
DESCRIBE STORAGE CREDENTIAL cockroachdb_cdc_storage_credential;
```

### Error: "INVALID_STORAGE_CREDENTIAL"
**Cause**: Azure storage account key is wrong or expired

**Solution**:
1. Get new key from Azure portal or credentials file
2. Update storage credential:
```sql
ALTER STORAGE CREDENTIAL cockroachdb_cdc_storage_credential
SET AZURE_STORAGE_ACCOUNT_KEY '<new-key>';
```

---

## Best Practices

### 1. Use Descriptive Names
```sql
-- Good
CREATE EXTERNAL LOCATION cockroachdb_cdc_parquet ...
CREATE EXTERNAL LOCATION cockroachdb_cdc_json ...

-- Bad
CREATE EXTERNAL LOCATION ext_loc_1 ...
CREATE EXTERNAL LOCATION my_location ...
```

### 2. Separate Locations by Environment
```sql
-- Production
CREATE EXTERNAL LOCATION cockroachdb_cdc_prod ...

-- Staging
CREATE EXTERNAL LOCATION cockroachdb_cdc_staging ...

-- Development
CREATE EXTERNAL LOCATION cockroachdb_cdc_dev ...
```

### 3. Use Groups for Permissions
```sql
-- Create groups
CREATE GROUP data_engineers;
CREATE GROUP data_analysts;

-- Grant to groups instead of individual users
GRANT READ FILES ON EXTERNAL LOCATION cockroachdb_cdc_parquet TO data_engineers;
GRANT READ FILES ON EXTERNAL LOCATION cockroachdb_cdc_parquet TO data_analysts;
```

### 4. Document External Locations
```sql
-- Add comments to explain purpose
CREATE EXTERNAL LOCATION cockroachdb_cdc_parquet
URL '...'
WITH (STORAGE CREDENTIAL ...)
COMMENT 'CockroachDB CDC Parquet files - Updated nightly by changefeed. Contact: data-team@company.com';
```

### 5. Audit Access
```sql
-- Check who has access
SHOW GRANTS ON EXTERNAL LOCATION cockroachdb_cdc_parquet;

-- Check what a user can access
SHOW GRANTS ON EXTERNAL LOCATION FOR `user@company.com`;
```

---

## Complete Setup Script

Copy this entire script and run it in a Databricks SQL notebook:

```sql
-- ============================================================================
-- Complete Unity Catalog External Location Setup for CockroachDB CDC
-- ============================================================================

-- Step 1: Create storage credential
CREATE STORAGE CREDENTIAL IF NOT EXISTS cockroachdb_cdc_storage_credential
WITH (
  AZURE_STORAGE_ACCOUNT_NAME 'cockroachcdc1766504458',
  AZURE_STORAGE_ACCOUNT_KEY '<paste-your-key-here>'
)
COMMENT 'Azure Blob Storage credentials for CockroachDB CDC changefeeds';

-- Step 2: Create external locations
CREATE EXTERNAL LOCATION IF NOT EXISTS cockroachdb_cdc_parquet
URL 'wasbs://changefeed-events@cockroachcdc1766504458.blob.core.windows.net/parquet-cdc'
WITH (STORAGE CREDENTIAL cockroachdb_cdc_storage_credential)
COMMENT 'CockroachDB CDC Parquet changefeeds from Azure Blob Storage';

CREATE EXTERNAL LOCATION IF NOT EXISTS cockroachdb_cdc_json
URL 'wasbs://changefeed-events@cockroachcdc1766504458.blob.core.windows.net/json-cdc'
WITH (STORAGE CREDENTIAL cockroachdb_cdc_storage_credential)
COMMENT 'CockroachDB CDC JSON changefeeds from Azure Blob Storage';

-- Step 3: Grant permissions to your user (replace with your email)
GRANT READ FILES ON EXTERNAL LOCATION cockroachdb_cdc_parquet TO `robert.lee@company.com`;
GRANT READ FILES ON EXTERNAL LOCATION cockroachdb_cdc_json TO `robert.lee@company.com`;

-- Step 4: Verify setup
SHOW EXTERNAL LOCATIONS LIKE 'cockroachdb_cdc%';
SHOW GRANTS ON EXTERNAL LOCATION cockroachdb_cdc_parquet;
```

---

## Migration Checklist

- [ ] Create storage credential in Unity Catalog
- [ ] Create external location(s) for Parquet/JSON paths
- [ ] Grant permissions to users/groups
- [ ] Test access with `dbutils.fs.ls()` or `spark.read`
- [ ] Update notebook to use external location
- [ ] Remove credential loading code from notebook
- [ ] Remove `.option("fs.azure.account.key...")` from Autoloader
- [ ] Test full Autoloader pipeline
- [ ] Update documentation to reference external location
- [ ] Remove `cockroachdb_cdc_azure.json` from notebook dependencies

---

## Summary

**Before (Credentials in Code)**:
- ❌ Storage key in notebook code
- ❌ Must update all notebooks if key rotates
- ❌ Risk of credential exposure in version control
- ❌ Each user needs credential file

**After (Unity Catalog External Location)**:
- ✅ No credentials in code
- ✅ Centralized credential management
- ✅ Automatic permission enforcement
- ✅ Easy credential rotation
- ✅ Audit trail of access

**Next Steps**: Follow Step 1-5 above to set up external locations, then update your `load_parquet_files.ipynb` notebook!





