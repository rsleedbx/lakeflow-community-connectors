# Databricks CLI Commands for Unity Catalog

This document shows the equivalent Databricks CLI/API commands for the SQL statements used to set up Unity Catalog for CockroachDB CDC.

## Prerequisites

```bash
# Install Databricks CLI
pip install databricks-cli

# Configure with your workspace
databricks configure --token
# Enter workspace URL and personal access token
```

## 1. Create Storage Credential with Managed Identity

### SQL Command (Databricks Notebook/SQL Editor)
```sql
CREATE STORAGE CREDENTIAL cockroachdb_cdc_credential
WITH (
  AZURE_MANAGED_IDENTITY (
    MANAGED_IDENTITY_ID '02d74063-b9d2-4984-819e-1db4f8345ada'
  )
);
```

### Databricks CLI Equivalent

**Method 1: Using JSON file**
```bash
cat > /tmp/storage_credential.json << 'EOF'
{
  "name": "cockroachdb_cdc_credential",
  "comment": "Azure ADLS Gen2 with Managed Identity for CockroachDB CDC",
  "azure_managed_identity": {
    "access_connector_id": "/subscriptions/{subscription_id}/resourceGroups/{rg}/providers/Microsoft.Databricks/accessConnectors/{connector_name}"
  },
  "read_only": false,
  "skip_validation": false
}
EOF

databricks unity-catalog storage-credentials create --json @/tmp/storage_credential.json
```

**Method 2: Using inline JSON**
```bash
databricks unity-catalog storage-credentials create --json '{
  "name": "cockroachdb_cdc_credential",
  "comment": "Azure ADLS Gen2 with Managed Identity",
  "azure_managed_identity": {
    "access_connector_id": "/subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.Databricks/accessConnectors/{connector}"
  },
  "read_only": false
}'
```

**Method 3: Check if credential exists**
```bash
databricks unity-catalog storage-credentials get cockroachdb_cdc_credential
```

**Method 4: List all credentials**
```bash
databricks unity-catalog storage-credentials list --output json | jq
```

---

## 2. Create External Location

### SQL Command (Databricks Notebook/SQL Editor)
```sql
CREATE EXTERNAL LOCATION cockroachdb_cdc_parquet
URL 'abfss://changefeed-events@cockroachcdc1767020732.dfs.core.windows.net/parquet-cdc'
WITH (STORAGE CREDENTIAL cockroachdb_cdc_credential);

CREATE EXTERNAL LOCATION cockroachdb_cdc_json
URL 'abfss://changefeed-events@cockroachcdc1767020732.dfs.core.windows.net/json-cdc'
WITH (STORAGE CREDENTIAL cockroachdb_cdc_credential);
```

### Databricks CLI Equivalent

**Method 1: Using command-line flags**
```bash
# Parquet location
databricks unity-catalog external-locations create \
  --name cockroachdb_cdc_parquet \
  --url 'abfss://changefeed-events@cockroachcdc1767020732.dfs.core.windows.net/parquet-cdc' \
  --credential-name cockroachdb_cdc_credential \
  --comment "CockroachDB CDC Parquet files" \
  --read-only false \
  --skip-validation false

# JSON location
databricks unity-catalog external-locations create \
  --name cockroachdb_cdc_json \
  --url 'abfss://changefeed-events@cockroachcdc1767020732.dfs.core.windows.net/json-cdc' \
  --credential-name cockroachdb_cdc_credential \
  --comment "CockroachDB CDC JSON files" \
  --read-only false \
  --skip-validation false
```

**Method 2: Using JSON file**
```bash
cat > /tmp/external_location.json << 'EOF'
{
  "name": "cockroachdb_cdc_parquet",
  "url": "abfss://changefeed-events@cockroachcdc1767020732.dfs.core.windows.net/parquet-cdc",
  "credential_name": "cockroachdb_cdc_credential",
  "comment": "CockroachDB CDC Parquet files",
  "read_only": false,
  "skip_validation": false
}
EOF

databricks unity-catalog external-locations create --json @/tmp/external_location.json
```

**Method 3: Check if location exists**
```bash
databricks unity-catalog external-locations get cockroachdb_cdc_parquet
```

**Method 4: List all external locations**
```bash
databricks unity-catalog external-locations list --output json | jq
```

---

## 3. Grant Permissions

### SQL Command (Databricks Notebook/SQL Editor)
```sql
GRANT READ FILES ON EXTERNAL LOCATION cockroachdb_cdc_parquet TO `robert.lee@databricks.com`;
GRANT READ FILES ON EXTERNAL LOCATION cockroachdb_cdc_json TO `robert.lee@databricks.com`;
```

### Databricks CLI Equivalent

**Method 1: Using grants update**
```bash
# Grant READ FILES permission on Parquet location
databricks unity-catalog grants update \
  --principal "robert.lee@databricks.com" \
  --securable-type EXTERNAL_LOCATION \
  --name cockroachdb_cdc_parquet \
  --changes '[{"add":["READ_FILES"]}]'

# Grant READ FILES permission on JSON location
databricks unity-catalog grants update \
  --principal "robert.lee@databricks.com" \
  --securable-type EXTERNAL_LOCATION \
  --name cockroachdb_cdc_json \
  --changes '[{"add":["READ_FILES"]}]'
```

**Method 2: List grants on external location**
```bash
databricks unity-catalog grants get \
  --securable-type EXTERNAL_LOCATION \
  --full-name cockroachdb_cdc_parquet
```

---

## 4. Complete Automation Script

The `setup_unity_catalog.sh` script automates all the above steps:

```bash
cd sources/cockroachdb/scripts
./setup_unity_catalog.sh
```

**What it does:**
1. ✅ Checks if storage credential exists, creates if not
2. ✅ Creates external locations for Parquet and JSON
3. ✅ Grants READ FILES permissions to current user
4. ✅ Provides usage examples for notebooks

**Features:**
- 🔐 Supports Azure Managed Identity (user-assigned)
- 🔑 Falls back to access key with manual UI instructions
- ✅ Idempotent (safe to run multiple times)
- 📋 Auto-detects credentials from `cockroachdb_cdc_azure.json`

---

## 5. Usage in Databricks Notebooks

After running `setup_unity_catalog.sh`, you can use the external locations in notebooks:

### Python (Spark)
```python
# Read Parquet CDC files using Unity Catalog external location
df = (spark.read
  .format("cloudFiles")
  .option("cloudFiles.format", "parquet")
  .option("cloudFiles.schemaLocation", "/tmp/schema/parquet")
  .load("azure-blob://cockroachdb_cdc_parquet"))

display(df)
```

### SQL
```sql
-- Create external table using external location
CREATE EXTERNAL TABLE IF NOT EXISTS cdc.users_snapshot
LOCATION 'azure-blob://cockroachdb_cdc_parquet'
```

### PySpark with abfss:// directly (requires storage credential)
```python
# Read using direct abfss:// URL (Unity Catalog resolves credentials)
df = (spark.read
  .format("cloudFiles")
  .option("cloudFiles.format", "parquet")
  .load("abfss://changefeed-events@cockroachcdc1767020732.dfs.core.windows.net/parquet-cdc"))
```

---

## 6. Troubleshooting

### Check Storage Credential Status
```bash
databricks unity-catalog storage-credentials get cockroachdb_cdc_credential
```

### Check External Location Status
```bash
databricks unity-catalog external-locations get cockroachdb_cdc_parquet
```

### List all permissions
```bash
# List grants on storage credential
databricks unity-catalog grants get \
  --securable-type STORAGE_CREDENTIAL \
  --full-name cockroachdb_cdc_credential

# List grants on external location
databricks unity-catalog grants get \
  --securable-type EXTERNAL_LOCATION \
  --full-name cockroachdb_cdc_parquet
```

### Delete and recreate (if needed)
```bash
# Delete external location
databricks unity-catalog external-locations delete cockroachdb_cdc_parquet

# Delete storage credential
databricks unity-catalog storage-credentials delete cockroachdb_cdc_credential
```

---

## 7. References

- [Databricks CLI Documentation](https://docs.databricks.com/dev-tools/cli/)
- [Unity Catalog Storage Credentials](https://docs.databricks.com/data-governance/unity-catalog/manage-external-locations-and-credentials.html)
- [Azure Managed Identity with Databricks](https://learn.microsoft.com/en-us/azure/databricks/security/aad-storage-service-principal)
- [External Locations in Unity Catalog](https://docs.databricks.com/data-governance/unity-catalog/create-external-locations.html)

---

## Summary

✅ **All SQL commands shown in your question can be executed via Databricks CLI:**

| SQL Command | Databricks CLI Command |
|------------|------------------------|
| `CREATE STORAGE CREDENTIAL ... WITH (AZURE_MANAGED_IDENTITY ...)` | `databricks unity-catalog storage-credentials create --json '{...}'` |
| `CREATE EXTERNAL LOCATION ... WITH (STORAGE CREDENTIAL ...)` | `databricks unity-catalog external-locations create --name ... --url ... --credential-name ...` |
| `GRANT READ FILES ON EXTERNAL LOCATION ...` | `databricks unity-catalog grants update --principal ... --changes '[{"add":["READ_FILES"]}]'` |

The `setup_unity_catalog.sh` script in this repo automates all of these steps! 🚀





