# Configuration Loading Examples

This document shows different scenarios for how the notebook loads configuration.

---

## 📋 How Configuration Loading Works

The notebook (Cell 2) tries to load configuration from multiple sources in this priority order:

```
1. Local .env directory → 2. Notebook directory → 3. Unity Catalog Volume → 4. Embedded fallback
```

---

## 🎯 Scenario 1: No External Config File (Default)

### Situation
- No config file in any of the search paths
- First time running the notebook

### What Happens
```
ℹ️  Using embedded configuration (no external config file found)

📊 Configuration Summary:
   CockroachDB: databricks-4303.75rf.aws-us-east-1.crdb.io:26257/defaultdb
   Source: defaultdb.public.usertable_append_only_multi_cf
   Target: main.robert_lee_cockroachdb.usertable_append_only_multi_cf
   Azure Storage: cockroachcdc1768934658/changefeed-events
...
```

### Result
✅ Notebook works immediately with embedded config

---

## 🎯 Scenario 2: Config File in Notebook Directory

### Situation
- You uploaded `cockroachdb_cdc_tutorial_config.json` to the same folder as your notebook

### What Happens
```
✅ Configuration loaded from: cockroachdb_cdc_tutorial_config.json

📊 Configuration Summary:
   CockroachDB: your-cluster.crdb.io:26257/defaultdb
   Source: defaultdb.public.usertable_append_only_single_cf
   Target: main.your_schema.usertable_append_only_single_cf
...
```

### Result
✅ Notebook uses your custom config from the file

---

## 🎯 Scenario 3: Config File in Unity Catalog Volume

### Situation
- You stored config in Unity Catalog Volume for team sharing
- Updated Cell 2 to uncomment the Unity Catalog path

### Code in Cell 2
```python
config_file_paths = [
    "/Users/robert.lee/github/lakeflow-community-connectors/sources/cockroachdb/.env/cockroachdb_cdc_tutorial_config.json",
    "cockroachdb_cdc_tutorial_config.json",
    "/Volumes/main/production/config/cockroachdb_cdc_tutorial_config.json",  # ← Uncommented
]
```

### What Happens
```
✅ Configuration loaded from: /Volumes/main/production/config/cockroachdb_cdc_tutorial_config.json

📊 Configuration Summary:
   CockroachDB: prod-cluster.crdb.io:26257/production
   Source: production.public.orders_append_only_single_cf
   Target: main.production_analytics.orders_append_only_single_cf
...
```

### Result
✅ Entire team uses shared production config from Unity Catalog

---

## 🎯 Scenario 4: Config File in Local .env (Development)

### Situation
- You're developing locally
- Config file is in `.env` directory (gitignored for security)

### What Happens
```
✅ Configuration loaded from: /Users/robert.lee/github/lakeflow-community-connectors/sources/cockroachdb/.env/cockroachdb_cdc_tutorial_config.json

📊 Configuration Summary:
   CockroachDB: localhost:26257/dev
   Source: dev.public.test_table_update_delete_single_cf
   Target: main.dev_testing.test_table_update_delete_single_cf
...
```

### Result
✅ Local development config is used, safe from git commits

---

## 🎯 Scenario 5: Multiple Config Files (Priority Order)

### Situation
- Config file exists in `.env` directory
- Config file also exists in notebook directory

### What Happens
```
✅ Configuration loaded from: /Users/robert.lee/github/lakeflow-community-connectors/sources/cockroachdb/.env/cockroachdb_cdc_tutorial_config.json
```

### Explanation
The notebook loads the **first valid config it finds**:
1. ✅ Found in `.env` → **LOADED** (stops searching)
2. ⏭️ Skipped notebook directory (already loaded)
3. ⏭️ Skipped Unity Catalog (already loaded)

### Result
✅ Local `.env` config takes priority over notebook directory

---

## 🎯 Scenario 6: Config File Error Handling

### Situation
- Config file exists but has JSON syntax error

### What Happens
```
⚠️  Failed to load cockroachdb_cdc_tutorial_config.json: Expecting property name enclosed in double quotes: line 5 column 3 (char 123)
ℹ️  Using embedded configuration (no external config file found)

📊 Configuration Summary:
   (embedded config values shown here)
...
```

### Result
✅ Notebook falls back to embedded config gracefully

---

## 🎯 Scenario 7: Switching Between Environments

### Situation
- Testing → Staging → Production
- Different config for each environment

### Approach 1: Rename Config Files
```bash
# On your local machine
cp cockroachdb_cdc_tutorial_config.test.json cockroachdb_cdc_tutorial_config.json
# Upload to Databricks
```

### Approach 2: Use Unity Catalog Volumes
```python
# In Cell 2, comment/uncomment as needed:
config_file_paths = [
    # "cockroachdb_cdc_tutorial_config.json",  # ← Test (commented out)
    # "/Volumes/main/staging/config/cockroachdb_cdc_tutorial_config.json",  # ← Staging (commented out)
    "/Volumes/main/production/config/cockroachdb_cdc_tutorial_config.json",  # ← Production (active)
]
```

### Approach 3: Edit Embedded Config
```python
# For quick testing, just edit the embedded config in Cell 2
config = {
  "cdc_config": {
    "mode": "update_delete",  # ← Changed from append_only
    ...
  }
}
```

---

## 🔒 Security Best Practices

### ✅ DO:
- Store production configs in Unity Catalog Volumes
- Use `.env` directory for local development (gitignored)
- Use Databricks Secrets for highly sensitive values
- Keep embedded config with placeholder values

### ❌ DON'T:
- Commit real credentials to git
- Share config files via email/Slack
- Store production configs in personal workspace

---

## 🐛 Troubleshooting

### Problem: "No such file or directory"
**Cause**: Config file path doesn't exist

**Solution**:
1. Check file is uploaded to Databricks
2. Verify path is correct in Cell 2
3. Use absolute path if relative path fails

### Problem: "JSON decode error"
**Cause**: Invalid JSON syntax in config file

**Solution**:
1. Validate JSON at https://jsonlint.com/
2. Check for missing commas, quotes, braces
3. Remove trailing commas (not allowed in JSON)

### Problem: "KeyError: 'cdc_config'"
**Cause**: Config file missing required sections

**Solution**:
1. Compare your config with `cockroachdb_cdc_tutorial_config.json` template
2. Ensure all required sections are present
3. Use embedded config as reference

---

## 📊 Config Loading Decision Tree

```
Start Cell 2
    ↓
Check .env directory
    ↓
   Yes → Load → Success?
    ↓           ↓
   No          Yes → DONE ✅
    ↓           ↓
Check notebook dir   No → Continue
    ↓
   Yes → Load → Success?
    ↓           ↓
   No          Yes → DONE ✅
    ↓           ↓
Check Unity Catalog  No → Continue
    ↓
   Yes → Load → Success?
    ↓           ↓
   No          Yes → DONE ✅
    ↓           ↓
Use embedded config  No → Continue
    ↓
  DONE ✅ (always works)
```

---

## 🎓 Summary

The notebook's configuration loading is designed to be:
- **Flexible**: Multiple sources, priority order
- **Robust**: Automatic fallback to embedded config
- **Secure**: Supports `.env` and Unity Catalog
- **Team-friendly**: Easy to share via Unity Catalog
- **Developer-friendly**: Works immediately with no setup

Choose the approach that best fits your workflow! 🚀
