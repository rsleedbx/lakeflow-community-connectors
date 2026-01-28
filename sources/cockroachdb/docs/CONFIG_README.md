# CockroachDB CDC Tutorial - Configuration Guide

This guide explains how to use the JSON configuration file for the CockroachDB CDC Tutorial notebook.

---

## 📁 Files

- **`cockroachdb_cdc_tutorial_config.json`** - Template configuration file
- **`cockroachdb_cdc_tutorial_config.example.json`** - Example with sample values
- **`notebook_config_loader.py`** - 🆕 Ready-to-use code for notebook Cell 2
- **`load_config_example.py`** - Example code snippet
- **`cockroachdb-cdc-tutorial.ipynb`** - Main tutorial notebook

---

## 🚀 Quick Start

### Option 1: Use Embedded Config (Fastest for Testing)

The notebook has an embedded configuration with your values. Just run Cell 2 and it will use the embedded config automatically.

```python
# Cell 2 will show:
# ℹ️  Using embedded configuration (no external config file found)
```

### Option 2: Use External Config File (Recommended for Production)

### Step 1: Copy and Edit Configuration

```bash
# Copy template to your local config
cp cockroachdb_cdc_tutorial_config.json my_config.json

# Edit with your credentials
vim my_config.json  # or use your favorite editor
```

### Step 2: Replace Placeholders

Edit `my_config.json` and replace all `REPLACE_WITH_YOUR_*` values:

```json
{
  "cockroachdb": {
    "host": "your-cluster.crdb.io",          // ← Replace
    "port": 26257,
    "user": "your-username",                 // ← Replace
    "password": "your-password",             // ← Replace
    "database": "defaultdb"
  },
  "azure_storage": {
    "account_name": "yourstorageaccount",    // ← Replace
    "account_key": "your-storage-key==",     // ← Replace
    "container_name": "changefeed-events"
  },
  "databricks_target": {
    "catalog": "main",
    "schema": "your_schema"                  // ← Replace
  }
}
```

### Step 3: Upload to Databricks

**Option A: Upload via Workspace UI**

1. Open your Databricks workspace
2. Navigate to the folder where your notebook is located
3. Click "Upload" → Select `my_config.json`
4. Upload file

**Option B: Upload via CLI**

```bash
databricks workspace import \
  /Users/your-email@databricks.com/my_config.json \
  --file my_config.json \
  --profile YOUR_PROFILE
```

### Step 4: Use in Notebook

The notebook **automatically** tries to load configuration from these locations (in order):

1. **Local `.env` directory** (for development):
   ```
   /Users/robert.lee/github/lakeflow-community-connectors/sources/cockroachdb/.env/cockroachdb_cdc_tutorial_config.json
   ```

2. **Same directory as notebook** (Databricks workspace):
   ```
   cockroachdb_cdc_tutorial_config.json
   ```

3. **Unity Catalog Volume** (optional, uncomment in Cell 2):
   ```
   /Volumes/main/your_schema/your_volume/cockroachdb_cdc_tutorial_config.json
   ```

4. **Embedded fallback**: If no external file is found, uses the embedded config in Cell 2

**How it works:**
- Cell 2 loops through these paths in order
- First file that exists and loads successfully is used
- If all fail or no file found, embedded config is used
- Clear messages show which source was used

**No code changes needed!** Just upload your config file to one of the locations above.

---

## 📋 Configuration Reference

### CockroachDB Connection

```json
{
  "cockroachdb": {
    "host": "your-cluster.crdb.io",  // CockroachDB hostname (no port)
    "port": 26257,                    // CockroachDB port (default: 26257)
    "user": "your-username",          // CockroachDB username
    "password": "your-password",      // CockroachDB password
    "database": "defaultdb"           // Database name
  }
}
```

### CockroachDB Source

```json
{
  "cockroachdb_source": {
    "catalog": "defaultdb",                         // Source catalog/database (usually defaultdb)
    "schema": "public",                             // Source schema (usually public)
    "table_name": "usertable_append_only_single_cf" // Source table name
  }
}
```

**Table Name Format**: `{base_name}_{mode}_{column_family_mode}`
- Example: `usertable_append_only_single_cf`
- Example: `usertable_update_delete_multi_cf`

### Azure Storage

```json
{
  "azure_storage": {
    "account_name": "yourstorageaccount",  // Azure Storage Account name
    "account_key": "your-storage-key==",   // Azure Storage Account Key
    "container_name": "changefeed-events"  // Container name
  }
}
```

### Databricks Target

```json
{
  "databricks_target": {
    "catalog": "main",                              // Target Unity Catalog
    "schema": "your_schema",                        // Target schema
    "table_name": "usertable_append_only_single_cf" // Target table name
  }
}
```

**Table Name Format**: `{base_name}_{mode}_{column_family_mode}`
- Example: `usertable_append_only_single_cf`
- Example: `usertable_update_delete_multi_cf`

### CDC Configuration

```json
{
  "cdc_config": {
    "mode": "append_only",                  // "append_only" or "update_delete"
    "column_family_mode": "single_cf",      // "single_cf" or "multi_cf"
    "primary_key_columns": ["ycsb_key"],    // List of primary key columns
    "auto_suffix_mode_family": true         // Auto-append mode/family suffix to table names
  }
}
```

**CDC Modes:**
- `append_only` - Append-only ingestion (SCD Type 2, keeps all history)
- `update_delete` - Update/delete mode (SCD Type 1, latest state only)

**Column Family Modes:**
- `single_cf` - No column families (standard tables)
- `multi_cf` - Multiple column families (requires CockroachDB `split_column_families` changefeed option)

**Auto-Suffix Table Names:**
- `auto_suffix_mode_family: true` - Automatically appends `_{mode}_{column_family_mode}` to table names if not already present
  - Example: `usertable` → `usertable_append_only_single_cf`
  - Example: `usertable_append_only_single_cf` → `usertable_append_only_single_cf` (no change)
- `auto_suffix_mode_family: false` - Use table names exactly as specified in config

### Workload Configuration

```json
{
  "workload_config": {
    "snapshot_count": 10,  // Initial rows (snapshot phase)
    "insert_count": 10,    // New rows to insert
    "update_count": 9,     // Existing rows to update
    "delete_count": 8      // Rows to delete
  }
}
```

**Net growth per cycle**: `insert_count - delete_count`
- Example: 10 inserts - 8 deletes = **+2 rows** per cycle

---

## 🔒 Security Best Practices

### 1. Never Commit Credentials

Add your config file to `.gitignore`:

```bash
echo "my_config.json" >> .gitignore
```

### 2. Use Databricks Secrets (Production)

For production use, use Databricks Secrets instead of JSON files:

```python
# Replace JSON config with secrets
cockroachdb_password = dbutils.secrets.get(scope="cockroachdb", key="password")
storage_account_key = dbutils.secrets.get(scope="azure", key="storage_key")
```

### 3. Restrict File Permissions

```bash
chmod 600 my_config.json  # Read/write for owner only
```

---

## 📝 Example Configurations

### Configuration 1: Append-Only, Single Column Family

```json
{
  "cockroachdb_source": {
    "catalog": "defaultdb",
    "schema": "public",
    "table_name": "usertable"  // Will become usertable_append_only_single_cf
  },
  "databricks_target": {
    "catalog": "main",
    "schema": "production_cdc",
    "table_name": "usertable"  // Will become usertable_append_only_single_cf
  },
  "cdc_config": {
    "mode": "append_only",
    "column_family_mode": "single_cf",
    "primary_key_columns": ["ycsb_key"],
    "auto_suffix_mode_family": true
  }
}
```

### Configuration 2: Update/Delete, Multiple Column Families

```json
{
  "cockroachdb_source": {
    "catalog": "defaultdb",
    "schema": "public",
    "table_name": "usertable"  // Will become usertable_update_delete_multi_cf
  },
  "databricks_target": {
    "catalog": "main",
    "schema": "production_cdc",
    "table_name": "usertable"  // Will become usertable_update_delete_multi_cf
  },
  "cdc_config": {
    "mode": "update_delete",
    "column_family_mode": "multi_cf",
    "primary_key_columns": ["ycsb_key"],
    "auto_suffix_mode_family": true
  }
}
```

### Configuration 3: Composite Primary Key (Manual Table Names)

```json
{
  "cockroachdb_source": {
    "catalog": "ecommerce",
    "schema": "production",
    "table_name": "orders"
  },
  "databricks_target": {
    "catalog": "main",
    "schema": "ecommerce_cdc",
    "table_name": "orders_delta"  // Custom name, no auto-suffix
  },
  "cdc_config": {
    "mode": "update_delete",
    "column_family_mode": "single_cf",
    "primary_key_columns": ["tenant_id", "order_id"],
    "auto_suffix_mode_family": false  // Disabled for custom naming
  }
}
```

---

## 🐛 Troubleshooting

### Error: "File not found: my_config.json"

**Solution**: Upload config file to same directory as notebook

```bash
# Check file location in Databricks
%fs ls /Workspace/Users/your-email@databricks.com/
```

### Error: "KeyError: 'cockroachdb'"

**Solution**: Check JSON syntax is correct

```bash
# Validate JSON locally
python -m json.tool my_config.json
```

### Error: "Invalid credentials"

**Solution**: Verify credentials are correct and not placeholder values

```python
# Check if using placeholders
if "REPLACE" in cockroachdb_host:
    raise ValueError("Please replace REPLACE_WITH_YOUR_* placeholders in config file!")
```

---

## 📚 Related Documentation

- **`cockroachdb-cdc-tutorial.ipynb`** - Main tutorial notebook
- **`stream-changefeed-to-databricks-azure.md`** - Conceptual guide
- **`MODE_COMPARISON.md`** - CDC mode comparison (append_only vs update_delete)
- **`CONNECTOR_EVOLUTION_STRATEGY.md`** - Architecture and testing strategy

---

**Last Updated**: January 28, 2026  
**Maintainer**: Lakeflow Community Connectors Team
