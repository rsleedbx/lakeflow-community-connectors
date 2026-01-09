# Unity Catalog Connections Require channel=preview for Community Connectors

**Date:** December 23, 2025  
**Issue:** Unity Catalog connection credentials not being passed to community connectors  
**Root Cause:** Missing `channel=preview` in DLT pipeline configuration  
**Status:** ✅ Fixed  
**Scope:** Community/custom connectors only

---

## 🎯 **Summary**

Unity Catalog connections with **community connectors** require the preview channel. Without explicitly setting `channel=preview` in the pipeline configuration, Unity Catalog connection parameters are not resolved or passed to custom connectors.

**Important:** This limitation is specific to community/custom connectors. Built-in Databricks connectors may work with UC connections on the stable channel.

**Critical Fix:**
```python
pipeline_config = {
    "name": "my_pipeline",
    "channel": "preview",  # ✅ REQUIRED for Unity Catalog connections!
    "configuration": {
        "connection_name": "my_uc_connection",
        ...
    }
}
```

---

## 🐛 **The Problem**

### **Symptoms:**
Connector only receives 3 parameters:
- `databricks.connection` (connection name only)
- `tablename`
- `tablenamelist`

All actual connection credentials missing:
- ❌ No `host`
- ❌ No `port`
- ❌ No `database`
- ❌ No `user`
- ❌ No `password`
- ❌ No `token`
- ❌ No `base_url`

### **Error:**
```
ValueError: Missing required connection parameters: host, database, user
```

---

## 🔍 **Root Cause Analysis**

### **What Unity Catalog Connections with Community Connectors Require:**

1. **Unity Catalog Connection exists** ✅
   - Created via `create_databricks_connection.sh`
   - Configured with all credentials
   - Connection name passed to pipeline

2. **Pipeline configured with connection name** ✅
   ```python
   "configuration": {
       "connection_name": "robert_lee_battle-walrus-11108"
   }
   ```

3. **Preview channel enabled** ❌ **THIS WAS MISSING!**
   ```python
   "channel": "preview"  # Required for community connectors with UC connections
   ```

### **Why channel=preview is Required for Community Connectors:**

| Channel | UC Connections with Community Connectors |
|---------|------------------------------------------|
| **`stable`** | ❌ Not supported - UC connections are not resolved for community connectors |
| **`preview`** | ✅ Supported - UC connections are resolved and passed to community connector |

Without `channel=preview`:
1. DLT uses stable runtime
2. Stable runtime doesn't support UC connections **for community connectors**
3. Connection name is passed but not resolved
4. Community connector receives connection name but no credentials
5. **Note:** Built-in connectors may work without preview channel

With `channel=preview`:
1. DLT uses preview runtime
2. Preview runtime supports UC connections **for community connectors**
3. Connection is resolved from Unity Catalog
4. **All credentials are passed to community connector**

---

## ✅ **The Fix**

### **Before (Not Working):**

```python
# createpipeline.sh - MISSING channel parameter
pipeline_config = {
    "name": f"{catalog}.{schema}.{pipeline_name}",
    # ❌ No channel specified - defaults to 'stable'
    "storage": f"{workspace_path}/storage",
    "configuration": {
        "connection_name": connection_name,
        ...
    }
}
```

**Result:** Connection name passed, but credentials NOT resolved.

---

### **After (Working):**

```python
# createpipeline.sh - WITH channel=preview
pipeline_config = {
    "name": f"{catalog}.{schema}.{pipeline_name}",
    "channel": "preview",  # ✅ REQUIRED!
    "storage": f"{workspace_path}/storage",
    "configuration": {
        "connection_name": connection_name,
        ...
    }
}
```

**Result:** Connection resolved from UC, all credentials passed to connector!

---

## 🧪 **Testing the Fix**

### **Before Fix:**
```python
# Connector receives only:
options = {
    "databricks.connection": "robert_lee_battle-walrus-11108",
    "tablename": "usertable",
    "tablenamelist": ["usertable"]
}
# Missing: host, port, database, user, password, token, base_url
```

### **After Fix:**
```python
# Connector receives all credentials:
options = {
    "databricks.connection": "robert_lee_battle-walrus-11108",
    "tablename": "usertable",
    "tablenamelist": ["usertable"],
    "token": "username:password",
    "base_url": "postgresql://host:26257/database?sslmode=require",
    "host": "battle-walrus-11108.jxf.gcp-us-east1.cockroachlabs.cloud",
    "port": "26257",
    "database": "ycsb",
    "user": "rslee",
    "password": "W5oTzuQNgpOR2KpQBljdCQ",
    "sslmode": "require",
    "schema": "public"
}
```

---

## 📋 **Files Updated**

### **1. createpipeline.sh**
```bash
# Line ~121
channel: "PREVIEW",  # ✅ Required for community connectors with UC connections
```

### **2. create_volume_pipeline.sh**
```bash
# Line ~76
"channel": "PREVIEW",  # ✅ Required for community connectors with UC connections
```

### **3. ingest.py (Best Practice)**
While not the root cause, connection name should still be dynamic:
```python
# Line ~151
pipeline_spec = {
    "connection_name": connection_name,  # ✅ Use variable, not hardcoded
    "objects": [...]
}
```

---

## ⚠️ **Important Scope Note**

This `channel=preview` requirement is **specific to community/custom connectors** using Unity Catalog connections.

**Community Connectors:** Custom Python connectors that implement the Lakeflow Connect interface
- ✅ Require `channel=preview` for UC connection support
- ✅ This includes CockroachDB, custom PostgreSQL, custom MySQL, etc.

**Built-in Connectors:** Databricks native connectors
- ⚠️ May work with UC connections on stable channel
- ⚠️ Check Databricks documentation for specific connector requirements

---

## ❌ **Common Misconceptions**

### **Misconception 1: Hardcoded connection name was the issue**
**Reality:** While `connection_name` should be dynamic (best practice), a hardcoded connection name that matches the actual UC connection would still work IF `channel=preview` is set.

### **Misconception 2: Deployment scripts were wrong**
**Reality:** Deployment scripts (copydir.sh, createpipeline.sh) were working correctly. The issue was in the pipeline configuration, not the deployment process.

### **Misconception 3: Unity Catalog connection was misconfigured**
**Reality:** The UC connection was correctly configured. The issue was that the DLT runtime wasn't using the preview channel that supports UC connections.

---

## 💡 **Key Learnings**

### **When debugging Unity Catalog connection issues with community connectors:**

1. **First check: Is `channel=preview` set?**
   - This is the most common cause of credential passing issues for community connectors
   - Without it, UC connections don't work with community/custom connectors in DLT
   - **Note:** Built-in connectors may not require this

2. **Second check: Does the UC connection exist?**
   ```bash
   databricks api --method GET --path /api/2.0/unity-catalog/connections
   ```

3. **Third check: Is the connection name correct?**
   - Connection name in pipeline config must match UC connection name
   - Use dynamic variable, not hardcoded string

4. **Fourth check: Are credentials in the UC connection correct?**
   - Test connection directly before using in pipeline

---

## 📊 **Impact**

### **Before Fix:**
- ❌ All Unity Catalog connection-based pipelines failed
- ❌ Connector couldn't connect to CockroachDB
- ❌ Error: Missing required connection parameters

### **After Fix:**
- ✅ Unity Catalog connections work correctly
- ✅ All credentials passed to connector
- ✅ Connector successfully connects to CockroachDB
- ✅ Pipeline runs end-to-end

---

## 🔗 **References**

- **Databricks Documentation:** [DLT Preview Channel](https://docs.databricks.com/delta-live-tables/preview.html)
- **Unity Catalog Connections:** [UC Connection Management](https://docs.databricks.com/connect/unity-catalog/index.html)
- **Related Fix:** [HARDCODED_CONNECTION_BUG.md](HARDCODED_CONNECTION_BUG.md) - This document (corrected)

---

## ✅ **Verification Checklist**

After applying the fix, verify:

- [ ] `channel: "preview"` is in createpipeline.sh
- [ ] `channel: "PREVIEW"` is in create_volume_pipeline.sh
- [ ] Pipeline configuration includes channel parameter
- [ ] UC connection exists and is accessible
- [ ] Connection name is dynamic (not hardcoded)
- [ ] Connector receives all expected parameters
- [ ] Pipeline runs successfully end-to-end

---

**Status:** ✅ **RESOLVED**  
**Last Updated:** December 23, 2025  
**Applies to:** Community/custom connectors using Unity Catalog connections in DLT pipelines

**Scope:** This requirement is specific to community connectors. Built-in Databricks connectors may not require `channel=preview` for Unity Catalog connections.

