# Unity Catalog Resource Names Reference

Quick reference for all Unity Catalog resources created by `setup_unity_catalog.sh`.

## 📋 Resource Names

### Storage Credential
```
Name: cockroachdb_cdc_storage_credential
Type: Azure Managed Identity
```

### External Locations

**Parquet CDC Files:**
```
Name: cockroachdb_cdc_parquet
URL:  abfss://changefeed-events@cockroachcdc1767020732.dfs.core.windows.net/parquet-cdc
```

**JSON CDC Files:**
```
Name: cockroachdb_cdc_json
URL:  abfss://changefeed-events@cockroachcdc1767020732.dfs.core.windows.net/json-cdc
```

---

## 🔍 Verification SQL Commands

Run these in Databricks SQL Editor to verify setup:

```sql
-- 1. Check storage credential
DESCRIBE STORAGE CREDENTIAL cockroachdb_cdc_storage_credential;

-- 2. Check external locations
DESCRIBE EXTERNAL LOCATION cockroachdb_cdc_parquet;
DESCRIBE EXTERNAL LOCATION cockroachdb_cdc_json;

-- 3. List all storage credentials
SHOW STORAGE CREDENTIALS;

-- 4. List all external locations
SHOW EXTERNAL LOCATIONS;

-- 5. Check permissions on external location
SHOW GRANTS ON EXTERNAL LOCATION cockroachdb_cdc_parquet;
SHOW GRANTS ON EXTERNAL LOCATION cockroachdb_cdc_json;
```

---

## 📱 Databricks Notebook Usage

### Method 1: Using External Location Name (Recommended)
```python
# Read Parquet files
df_parquet = (spark.read
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", "/tmp/schema/parquet")
    .load("azure-blob://cockroachdb_cdc_parquet"))

# Read JSON files
df_json = (spark.read
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/tmp/schema/json")
    .load("azure-blob://cockroachdb_cdc_json"))
```

### Method 2: Using Direct abfss:// URL
```python
# Unity Catalog automatically resolves credentials
df_parquet = spark.read.parquet(
    "abfss://changefeed-events@cockroachcdc1767020732.dfs.core.windows.net/parquet-cdc"
)

df_json = spark.read.json(
    "abfss://changefeed-events@cockroachcdc1767020732.dfs.core.windows.net/json-cdc"
)
```

### Method 3: List Files
```python
# Using external location
files = dbutils.fs.ls("azure-blob://cockroachdb_cdc_parquet/")
print(f"Found {len(files)} files in Parquet location")

# Using direct URL
files = dbutils.fs.ls("abfss://changefeed-events@cockroachcdc1767020732.dfs.core.windows.net/parquet-cdc/")
print(f"Found {len(files)} files")
```

---

## 🔄 Manual Creation (if CLI fails)

### 1. Create Storage Credential

**Via Databricks SQL:**
```sql
CREATE STORAGE CREDENTIAL cockroachdb_cdc_storage_credential
WITH (
  AZURE_MANAGED_IDENTITY (
    MANAGED_IDENTITY_ID '02d74063-b9d2-4984-819e-1db4f8345ada'
  )
);
```

**Via Databricks UI:**
1. Go to: **Catalog → External Data → Storage Credentials**
2. Click **"Create Credential"**
3. Fill in:
   - **Name:** `cockroachdb_cdc_storage_credential`
   - **Type:** Azure Managed Identity
   - **Managed Identity Client ID:** `02d74063-b9d2-4984-819e-1db4f8345ada`
4. Click **"Create"**

---

### 2. Create External Locations

**Via Databricks SQL:**
```sql
-- Parquet location
CREATE EXTERNAL LOCATION cockroachdb_cdc_parquet
URL 'abfss://changefeed-events@cockroachcdc1767020732.dfs.core.windows.net/parquet-cdc'
WITH (STORAGE CREDENTIAL cockroachdb_cdc_storage_credential);

-- JSON location
CREATE EXTERNAL LOCATION cockroachdb_cdc_json
URL 'abfss://changefeed-events@cockroachcdc1767020732.dfs.core.windows.net/json-cdc'
WITH (STORAGE CREDENTIAL cockroachdb_cdc_storage_credential);
```

**Via Databricks CLI:**
```bash
# Parquet location
databricks unity-catalog external-locations create \
  --name cockroachdb_cdc_parquet \
  --url 'abfss://changefeed-events@cockroachcdc1767020732.dfs.core.windows.net/parquet-cdc' \
  --credential-name cockroachdb_cdc_storage_credential

# JSON location
databricks unity-catalog external-locations create \
  --name cockroachdb_cdc_json \
  --url 'abfss://changefeed-events@cockroachcdc1767020732.dfs.core.windows.net/json-cdc' \
  --credential-name cockroachdb_cdc_storage_credential
```

---

### 3. Grant Permissions

**Via Databricks SQL:**
```sql
GRANT READ FILES ON EXTERNAL LOCATION cockroachdb_cdc_parquet TO `robert.lee@databricks.com`;
GRANT READ FILES ON EXTERNAL LOCATION cockroachdb_cdc_json TO `robert.lee@databricks.com`;
```

**Via Databricks CLI:**
```bash
databricks unity-catalog grants update \
  --principal "robert.lee@databricks.com" \
  --securable-type EXTERNAL_LOCATION \
  --name cockroachdb_cdc_parquet \
  --changes '[{"add":["READ_FILES"]}]'

databricks unity-catalog grants update \
  --principal "robert.lee@databricks.com" \
  --securable-type EXTERNAL_LOCATION \
  --name cockroachdb_cdc_json \
  --changes '[{"add":["READ_FILES"]}]'
```

---

## 🗑️ Cleanup (if needed)

```sql
-- Drop external locations first
DROP EXTERNAL LOCATION IF EXISTS cockroachdb_cdc_parquet;
DROP EXTERNAL LOCATION IF EXISTS cockroachdb_cdc_json;

-- Then drop storage credential
DROP STORAGE CREDENTIAL IF EXISTS cockroachdb_cdc_storage_credential;
```

Or via CLI:
```bash
databricks unity-catalog external-locations delete cockroachdb_cdc_parquet
databricks unity-catalog external-locations delete cockroachdb_cdc_json
databricks unity-catalog storage-credentials delete cockroachdb_cdc_storage_credential
```

---

## 📚 Additional Resources

- [Databricks Unity Catalog Documentation](https://docs.databricks.com/data-governance/unity-catalog/)
- [Azure Managed Identity with Databricks](https://learn.microsoft.com/en-us/azure/databricks/security/aad-storage-service-principal)
- [External Locations](https://docs.databricks.com/data-governance/unity-catalog/create-external-locations.html)
- [DATABRICKS_CLI_REFERENCE.md](./DATABRICKS_CLI_REFERENCE.md) - Complete CLI reference





