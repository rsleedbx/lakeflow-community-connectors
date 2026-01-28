# 🔄 Notebook Configuration Migration Guide

## Overview

This guide shows how to replace the hardcoded configuration in Cell 2 of `cockroachdb-cdc-tutorial.ipynb` with a JSON-based configuration system.

---

## ✅ What's New

### Before (Hardcoded)
```python
# CockroachDB connection (replace with your credentials)
cockroachdb_host = "databricks-4303.75rf.aws-us-east-1.crdb.io"
cockroachdb_port = 26257
cockroachdb_user = "robert"
cockroachdb_password = "Kib_4FYxyrU696n_SZEWqQ"
# ... 50 lines of hardcoded values ...
```

### After (JSON-Based)
```python
import json
from urllib.parse import quote

# Load configuration from JSON file
with open("cockroachdb_cdc_tutorial_config.json", "r") as f:
    config = json.load(f)

# All variables automatically extracted from JSON
cockroachdb_host = config["cockroachdb"]["host"]
# ... clean, maintainable code ...
```

---

## 🎯 Benefits

1. **✅ Security**: No hardcoded credentials in notebook
2. **✅ Portability**: Easy to share notebook without exposing secrets
3. **✅ Maintainability**: Update config once, use everywhere
4. **✅ Team Collaboration**: Each team member has their own config file
5. **✅ Documentation**: Config structure is self-documenting with comments

---

## 🚀 Migration Steps

### Step 1: Create Your Config File

1. **Copy the template:**
   ```bash
   cp cockroachdb_cdc_tutorial_config.json my_config.json
   ```

2. **Edit `my_config.json`** and replace all `REPLACE_WITH_YOUR_*` placeholders:
   ```json
   {
     "cockroachdb": {
       "host": "YOUR_ACTUAL_HOST",
       "user": "YOUR_ACTUAL_USER",
       "password": "YOUR_ACTUAL_PASSWORD"
     },
     "azure_storage": {
       "account_name": "YOUR_STORAGE_ACCOUNT",
       "account_key": "YOUR_STORAGE_KEY"
     },
     "databricks_target": {
       "schema": "YOUR_SCHEMA"
     }
   }
   ```

3. **Verify your config:**
   ```bash
   # Check that all REPLACE_WITH_YOUR_* are gone
   grep -i "REPLACE_WITH" my_config.json
   # Should return nothing if all replaced
   ```

### Step 2: Upload to Databricks

**Option A: Workspace Upload (Recommended)**

1. Open Databricks workspace
2. Navigate to folder with your notebook
3. Click "Upload" → Select `my_config.json`
4. Confirm it's in the same directory as the notebook

**Option B: Unity Catalog Volume**

1. Upload to a Unity Catalog Volume:
   ```sql
   -- Create volume if not exists
   CREATE VOLUME IF NOT EXISTS main.your_schema.config_files;
   ```

2. Upload via UI or CLI
3. Update `config_file_path` in notebook code:
   ```python
   config_file_path = "/Volumes/main/your_schema/config_files/my_config.json"
   ```

### Step 3: Update Notebook Cell 2

**🎯 Easiest Method: Copy Pre-Built Code**

1. **Open `notebook_config_loader.py`**
2. **Copy the entire file contents** (Cmd/Ctrl+A, Cmd/Ctrl+C)
3. **Open `cockroachdb-cdc-tutorial.ipynb`**
4. **Go to Cell 2** (the configuration cell)
5. **Delete all existing code** in Cell 2
6. **Paste the new code** from `notebook_config_loader.py`
7. **Adjust `config_file_path`** if needed:
   ```python
   # Same directory as notebook
   config_file_path = "my_config.json"
   
   # OR Unity Catalog Volume
   config_file_path = "/Volumes/main/your_schema/config_files/my_config.json"
   ```

### Step 4: Test

1. **Run Cell 2** in the notebook
2. **Verify output:**
   ```
   ✅ Configuration loaded from my_config.json
   
   📊 Configuration Summary:
      CockroachDB: your-host.crdb.io:26257/defaultdb
      Source: defaultdb.public.usertable_append_only_single_cf
      Target: main.your_schema.usertable_append_only_single_cf
      Azure Storage: yourstorageaccount/changefeed-events
   
   🔧 CDC Settings:
      Processing Mode: append_only
      Column Family Mode: single_cf
      Primary Keys: ['ycsb_key']
   
   📈 Workload:
      10 snapshot → +10 INSERTs, ~9 UPDATEs, -8 DELETEs
      Net growth per cycle: 2 rows
   ```

3. **Continue with rest of notebook** - all cells should work as before!

---

## 📁 File Reference

- **`cockroachdb_cdc_tutorial_config.json`** - Template with `REPLACE_WITH_YOUR_*` placeholders
- **`cockroachdb_cdc_tutorial_config.example.json`** - Example with sample values (for reference)
- **`notebook_config_loader.py`** - 🆕 Complete code for notebook Cell 2
- **`load_config_example.py`** - Minimal example (for custom use cases)
- **`CONFIG_README.md`** - Detailed configuration reference

---

## 🔒 Security Best Practices

### ✅ DO:
- Keep `my_config.json` private (never commit to git)
- Use Unity Catalog Volumes for production configs
- Use Databricks Secrets for highly sensitive values
- Add `*_config.json` to `.gitignore`

### ❌ DON'T:
- Commit config files with real credentials to git
- Share config files in public channels (Slack, email)
- Use the example file (`*.example.json`) with real credentials

### 🛡️ Extra Security: Use Databricks Secrets

For production, consider using Databricks Secrets:

```python
# In notebook Cell 2, replace direct config values with secrets
from pyspark.dbutils import DBUtils
dbutils = DBUtils(spark)

cockroachdb_password = dbutils.secrets.get(scope="crdb", key="password")
storage_account_key = dbutils.secrets.get(scope="azure", key="storage_key")

# Keep everything else from JSON
with open("cockroachdb_cdc_tutorial_config.json", "r") as f:
    config = json.load(f)
    
# Override sensitive values with secrets
config["cockroachdb"]["password"] = cockroachdb_password
config["azure_storage"]["account_key"] = storage_account_key
```

---

## 🤝 Team Collaboration

**Scenario**: Multiple team members working with different environments

1. **Commit to Git:**
   - ✅ `cockroachdb_cdc_tutorial_config.json` (template)
   - ✅ `cockroachdb_cdc_tutorial_config.example.json` (example)
   - ✅ `notebook_config_loader.py` (loader code)
   - ✅ `cockroachdb-cdc-tutorial.ipynb` (notebook with JSON loader in Cell 2)
   - ❌ `my_config.json` (personal config - each person creates their own)

2. **Each Team Member:**
   ```bash
   # Clone repo
   git clone your-repo
   
   # Create personal config
   cd sources/cockroachdb/docs/
   cp cockroachdb_cdc_tutorial_config.json my_config.json
   
   # Edit with personal credentials
   vim my_config.json
   
   # Upload to their own Databricks workspace
   # Run notebook - works with their config!
   ```

3. **`.gitignore` Entry:**
   ```gitignore
   # Ignore personal config files
   *_config.json
   !cockroachdb_cdc_tutorial_config.json
   !cockroachdb_cdc_tutorial_config.example.json
   ```

---

## 🐛 Troubleshooting

### Error: `FileNotFoundError: cockroachdb_cdc_tutorial_config.json`

**Cause**: Config file not found in expected location

**Solution**:
1. Verify file is uploaded to Databricks
2. Check file is in same directory as notebook
3. Or use absolute path:
   ```python
   config_file_path = "/Workspace/Users/your-email@company.com/my_config.json"
   ```

### Error: `KeyError: 'table_name'`

**Cause**: Using old config structure (before migration)

**Solution**: Update your config file to new structure with `table_name` fields:
```json
{
  "cockroachdb_source": {
    "catalog": "defaultdb",
    "schema": "public",
    "table_name": "usertable_append_only_single_cf"  // ← Add this
  },
  "databricks_target": {
    "catalog": "main",
    "schema": "your_schema",
    "table_name": "usertable_append_only_single_cf"  // ← Add this
  }
}
```

### Warning: `REPLACE_WITH_YOUR_*` still in config

**Cause**: Forgot to replace placeholder values

**Solution**: Edit your config file and replace all placeholders with actual values

---

## ✨ What's Next?

After migration, your notebook is:
- ✅ More secure (no hardcoded secrets)
- ✅ More maintainable (config in one place)
- ✅ More shareable (team members can use their own configs)
- ✅ Production-ready (can integrate with Databricks Secrets)

Continue with the rest of the tutorial - all subsequent cells work exactly the same! 🚀
