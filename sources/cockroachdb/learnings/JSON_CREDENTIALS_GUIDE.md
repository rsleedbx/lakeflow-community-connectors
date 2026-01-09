# JSON Credentials Guide

**Date:** 2025-12-23  
**Change:** Migrated from `.env` files to JSON credentials  
**Reason:** Better for notebooks, Autoloader examples, and consistency with v2.0 implementation

---

## 🎯 **Why JSON Instead of .env?**

| Format | Pros | Cons | Best For |
|--------|------|------|----------|
| **JSON** | ✅ Native to notebooks<br>✅ Easier to parse in Python<br>✅ Type-safe<br>✅ Consistent with examples<br>✅ Works with Databricks secrets | ❌ Requires JSON parser in bash | Notebooks, Autoloader, Python scripts |
| **.env** | ✅ Easy to source in bash<br>✅ Standard for shell scripts | ❌ Needs parsing in Python<br>❌ Not type-safe<br>❌ Inconsistent with v2.0 docs | Bash scripts only |

**Decision:** Use JSON as primary format, with bash helper script for legacy scripts.

---

## 📁 **File Locations**

### **JSON Credentials (Preferred)**

```
sources/cockroachdb/
├── examples/                                    # Templates (committed to git)
│   ├── credentials_parquet.json                 # Parquet format template
│   ├── credentials_json.json                    # JSON format template
│   └── credentials_both.json                    # Dual format template
└── .env/                                        # Your actual credentials (gitignored)
    └── credentials.json                         # Your local credentials
```

### **.env Files (Legacy, for bash scripts)**

```
sources/cockroachdb/.env/
├── cockroachdb_cockroachcloud.env              # CockroachDB connection
└── cockroachdb_cdc_azure.env                   # Azure storage
```

---

## 🆕 **JSON Credential Format**

### **Template:**

```json
{
  "catalog": "ecommerce",
  "schema": "public",
  "format": "parquet",
  
  "token": "username:password",
  "base_url": "postgresql://host:26257/ecommerce?sslmode=require",
  
  "azure_storage_account": "cockroachcdc",
  "azure_storage_key": "your-key-here",
  "azure_storage_container": "changefeed-events"
}
```

### **Required Fields (v2.0):**
- `catalog` - Database/catalog name
- `schema` - Schema name

### **Optional Fields:**
- `format` - Changefeed format ('parquet', 'json', or 'both', default: 'parquet')

### **Connection Fields:**
- `token` - Username:password for CockroachDB
- `base_url` - Connection string without credentials

### **Azure Fields (for Azure mode):**
- `azure_storage_account` - Azure storage account name
- `azure_storage_key` - Azure storage account key
- `azure_storage_container` - Container name (default: 'changefeed-events')

---

## 💻 **Usage Examples**

### **1. Python (Notebooks)**

```python
import json

# Load credentials
with open('sources/cockroachdb/.env/credentials.json') as f:
    credentials = json.load(f)

# Use directly with connector
from cockroachdb import LakeflowConnect
connector = LakeflowConnect(credentials)
```

**Output:**
```
Mode: Azure Parquet | Container: changefeed-events
Auto-constructed path: parquet/ecommerce/public/
```

---

### **2. Bash (Using yq)**

Create helper script `load_credentials.sh`:

```bash
#!/bin/bash
# Load JSON credentials into bash associative array

declare -gA credentials

# Use yq to read JSON into associative array
while IFS='=' read -r key value; do
    value="${value%\'}"
    value="${value#\'}"
    credentials["$key"]="$value"
done < <(yq -o=shell credentials.json)

# Export as environment variables
export CATALOG="${credentials[catalog]}"
export SCHEMA="${credentials[schema]}"
export FORMAT="${credentials[format]}"
export AZURE_STORAGE_ACCOUNT="${credentials[azure_storage_account]}"
export AZURE_STORAGE_KEY="${credentials[azure_storage_key]}"
export AZURE_STORAGE_CONTAINER="${credentials[azure_storage_container]}"

# Construct auto-generated path
export AUTO_PATH_PREFIX="${FORMAT}/${CATALOG}/${SCHEMA}"

echo "✅ Loaded: ${AUTO_PATH_PREFIX}"
```

**Usage:**
```bash
source sources/cockroachdb/scripts/load_credentials.sh
echo "Catalog: ${credentials[catalog]}"
echo "Schema: ${credentials[schema]}"
echo "Auto path: $AUTO_PATH_PREFIX"
```

**Output:**
```
✅ Loaded: parquet/ecommerce/public/
Catalog: ecommerce
Schema: public
Auto path: parquet/ecommerce/public/
```

---

### **3. Databricks Notebook**

```python
# Load from Databricks secrets (recommended) or direct file
import json

# Option 1: From file (development)
with open('/Volumes/main/workspace/credentials.json') as f:
    credentials = json.load(f)

# Option 2: From Databricks secrets (production)
credentials = {
    "catalog": dbutils.secrets.get("crdb", "catalog"),
    "schema": dbutils.secrets.get("crdb", "schema"),
    "format": "parquet",
    "token": dbutils.secrets.get("crdb", "token"),
    "base_url": dbutils.secrets.get("crdb", "base_url"),
    "azure_storage_account": dbutils.secrets.get("azure", "account"),
    "azure_storage_key": dbutils.secrets.get("azure", "key"),
    "azure_storage_container": "changefeed-events"
}

# Use with connector
from cockroachdb import LakeflowConnect
connector = LakeflowConnect(credentials)
```

---

### **4. Databricks Autoloader**

```python
# Construct path from JSON config
import json

with open('/Volumes/main/workspace/credentials.json') as f:
    config = json.load(f)

# Auto-construct path
source_path = f"wasbs://{config['azure_storage_container']}@{config['azure_storage_account']}.blob.core.windows.net/{config['format']}/{config['catalog']}/{config['schema']}/orders/"

# Use with Autoloader
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", config['format'])
    .option("cloudFiles.schemaLocation", "/checkpoints/schema")
    .load(source_path)
)
```

---

## 🔄 **Migration from .env to JSON**

### **Step 1: Run Updated Setup Script**

```bash
cd sources/cockroachdb/scripts
./setup_azure_blob_for_cdc.sh
```

This now creates **both**:
- `.env/cockroachdb_cdc_azure.env` (legacy)
- `.env/credentials.json` (new)

### **Step 2: Edit JSON Credentials**

```bash
vim sources/cockroachdb/.env/credentials.json
```

Update these fields:
```json
{
  "catalog": "ycsb",           # Your database name
  "schema": "public",
  "format": "parquet",
  
  "token": "rslee:password",   # Your CockroachDB credentials
  "base_url": "postgresql://host:26257/ycsb?sslmode=require",
  
  "azure_storage_account": "cockroachcdc...",  # Already filled
  "azure_storage_key": "...",                  # Already filled
  "azure_storage_container": "changefeed-events"
}
```

### **Step 3: Test with Notebook**

```bash
jupyter notebook sources/cockroachdb/unittest/cockroachdb.ipynb
```

The notebook now automatically loads from JSON!

**Expected output:**
```
✅ Loaded credentials from: credentials.json
================================================================================
LOADED CONFIGURATION
================================================================================
{
  "catalog": "ycsb",
  "schema": "public",
  "format": "parquet",
  "token": "***REDACTED***",
  "base_url": "postgresql://...",
  "azure_storage_account": "cockroachcdc...",
  "azure_storage_key": "***REDACTED***",
  "azure_storage_container": "changefeed-events"
}
================================================================================
✅ Credentials ready | Modes: direct_mode, azure_parquet_mode, volume_mode
   📁 Catalog: ycsb
   📂 Schema: public
   📊 Format: parquet
```

---

## 🔧 **Bash Scripts Using JSON**

### **Install yq:**

```bash
# macOS
brew install yq

# Linux
snap install yq

# Check version
yq --version
```

### **Use load_credentials.sh:**

```bash
# In your bash script
source sources/cockroachdb/scripts/load_credentials.sh

# Access credentials
echo "Catalog: ${credentials[catalog]}"
echo "Schema: ${credentials[schema]}"
echo "Format: ${credentials[format]}"
echo "Azure Account: $AZURE_STORAGE_ACCOUNT"

# Use auto-constructed path
echo "Path: $AUTO_PATH_PREFIX"
# Output: Path: parquet/ycsb/public
```

### **Example: Create Changefeed**

```bash
#!/bin/bash
source sources/cockroachdb/scripts/load_credentials.sh

# Construct Azure URI with auto-generated path
AZURE_URI="azure://${AZURE_STORAGE_CONTAINER}/${AUTO_PATH_PREFIX}/?AZURE_ACCOUNT_NAME=${AZURE_STORAGE_ACCOUNT}&AZURE_ACCOUNT_KEY=${AZURE_STORAGE_KEY}"

# Create changefeed
psql "$COCKROACHDB_BASE_URL" << EOF
CREATE CHANGEFEED FOR TABLE ${CATALOG}.${SCHEMA}.orders
INTO '$AZURE_URI'
WITH 
  format = '${FORMAT}',
  compression = 'gzip',
  updated,
  resolved = '10s';
EOF

echo "✅ Changefeed created: ${FORMAT}/${CATALOG}/${SCHEMA}/orders/"
```

---

## 📋 **Checklist**

- [x] Setup script creates JSON credentials
- [x] Notebook loads from JSON
- [x] `load_credentials.sh` helper for bash scripts
- [x] Examples directory has JSON templates
- [x] Documentation updated
- [x] .gitignore excludes `.env/credentials.json`

---

## 🔗 **Related Files**

- **Templates:** `sources/cockroachdb/examples/credentials_*.json`
- **Your credentials:** `sources/cockroachdb/.env/credentials.json` (gitignored)
- **Bash helper:** `sources/cockroachdb/scripts/load_credentials.sh`
- **Setup script:** `sources/cockroachdb/scripts/setup_azure_blob_for_cdc.sh`
- **Test notebook:** `sources/cockroachdb/unittest/cockroachdb.ipynb`

---

**Last Updated:** 2025-12-23  
**Status:** ✅ Complete - JSON is now the primary credential format




