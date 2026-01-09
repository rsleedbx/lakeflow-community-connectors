# Notebook v2.0 Updates

**Date:** 2025-12-23  
**File:** `sources/cockroachdb/unittest/cockroachdb.ipynb`  
**Status:** ✅ Updated to v2.0

---

## 🎯 **Changes Made**

### **1. Title Updated**
- **Old:** "CockroachDB Connector Unit Tests"
- **New:** "CockroachDB Connector Unit Tests (v2.0)"

### **2. Added Breaking Changes Notice**
Added prominent warning about v2.0 breaking changes:
- REQUIRED: `catalog` parameter
- REQUIRED: `schema` parameter  
- OPTIONAL: `format` parameter
- Paths are now auto-constructed

### **3. Credentials Structure Updated**

#### **Direct Mode:**
```python
# OLD (v1.0)
credentials["direct_mode"] = {
    "token": cockroach_token,
    "base_url": cockroach_base_url,
    "database": "ycsb",
    "schema": "public"  # Only schema, no catalog
}

# NEW (v2.0)
credentials["direct_mode"] = {
    # NEW REQUIRED PARAMETERS
    "catalog": database,  # Database name as catalog
    "schema": "public",
    "format": "parquet",  # New: format selection
    
    # Connection parameters
    "token": cockroach_token,
    "base_url": cockroach_base_url,
    "database": database,
    ...
}
```

#### **Azure Parquet Mode:**
```python
# OLD (v1.0)
credentials["azure_parquet_mode"] = {
    **credentials["direct_mode"],
    "azure_account_name": azure_account,
    "azure_account_key": azure_key,
    "azure_container": azure_container,
    "azure_path_prefix": "parquet-cdc"  # Manual path
}

# NEW (v2.0)
credentials["azure_parquet_mode"] = {
    **credentials["direct_mode"],
    # Azure storage credentials
    "azure_account_name": azure_account,
    "azure_account_key": azure_key,
    "azure_container": azure_container
    # NOTE: azure_path_prefix is now AUTO-CONSTRUCTED as: format/catalog/schema/
}
```

#### **Volume Mode:**
```python
# OLD (v1.0)
credentials["volume_mode"] = {
    "volume_path": "/Volumes/main/robert_lee_cockroachdb/parquet_files",
    "schema": "public"
}

# NEW (v2.0)
credentials["volume_mode"] = {
    # NEW REQUIRED PARAMETERS
    "catalog": "main",  # Volume catalog
    "schema": "robert_lee_cockroachdb",  # Volume schema
    
    # Volume path
    "volume_path": "/Volumes/main/robert_lee_cockroachdb/parquet_files"
}
```

### **4. Enhanced Output**
Added catalog/schema/format display:
```python
print(f"✅ Credentials ready | Modes: {', '.join(credentials.keys())}")
if credentials.get("direct_mode"):
    print(f"   📁 Catalog: {credentials['direct_mode'].get('catalog', 'N/A')}")
    print(f"   📂 Schema: {credentials['direct_mode'].get('schema', 'N/A')}")
    print(f"   📊 Format: {credentials['direct_mode'].get('format', 'N/A')}")
```

---

## 📊 **Expected Output After Update**

### **Cell 4: Credentials Setup**
```
✅ Credentials ready | Modes: direct_mode, azure_parquet_mode, volume_mode
   📁 Catalog: ycsb
   📂 Schema: public
   📊 Format: parquet
```

### **Cell 6: Direct Mode Test**
```
================================================================================
TEST 1: Direct Mode Initialization
================================================================================

📋 Options:
  catalog: ycsb
  schema: public
  format: parquet
  token: rslee:***REDACTED***
  base_url: postgresql://battle-walrus-11108...
  ...

Mode: Direct Sinkless | Database: ycsb

✅ Connector initialized successfully!
   Mode: direct
   Host: battle-walrus-11108.jxf.gcp-us-east1.cockroachlabs.cloud
   Database: ycsb
   Catalog: ycsb
   Schema: public
   Format: parquet
```

### **Cell 8: Azure Parquet Mode Test**
```
================================================================================
TEST 2: Azure Parquet Mode Initialization
================================================================================

📋 Options:
  catalog: ycsb
  schema: public
  format: parquet
  ...
  azure_account_name: cockroachcdc...
  azure_container: changefeed-events

Mode: Azure Parquet | Container: changefeed-events
Path will be: parquet/ycsb/public/

✅ Connector initialized successfully!
   Mode: azure_parquet
   Container: changefeed-events
   Catalog: ycsb
   Schema: public
   Format: parquet
   Auto-constructed path: parquet/ycsb/public/
```

### **Cell 10: Volume Mode Test**
```
================================================================================
TEST 3: Volume Mode Initialization
================================================================================

📋 Options:
  catalog: main
  schema: robert_lee_cockroachdb
  volume_path: /Volumes/main/robert_lee_cockroachdb/parquet_files

Mode: Volume | Path: /Volumes/main/robert_lee_cockroachdb/parquet_files

✅ Connector initialized successfully!
   Mode: volume
   Volume Path: /Volumes/main/robert_lee_cockroachdb/parquet_files
   Catalog: main
   Schema: robert_lee_cockroachdb
```

---

## 🧪 **Testing the Updated Notebook**

### **Step 1: Ensure .env files are up to date**
```bash
cd sources/cockroachdb/.env
# Check both files exist:
ls -la cockroachdb_cockroachcloud.env
ls -la cockroachdb_cdc_azure.env
```

### **Step 2: Run the notebook**
1. Open `sources/cockroachdb/unittest/cockroachdb.ipynb`
2. Run Cell 2 (Setup & imports)
3. Verify: "✅ Loaded CockroachDB credentials" and "✅ Loaded Azure credentials"
4. Run Cell 4 (Build credentials)
5. Verify output shows:
   - `Catalog: ycsb`
   - `Schema: public`
   - `Format: parquet`
6. Run Cell 6 (Direct Mode test)
7. Verify connector initializes successfully
8. Run Cell 8 (Azure Parquet Mode test)
9. Verify auto-constructed path: `parquet/ycsb/public/`

### **Step 3: Test format switching**
In Cell 4, change:
```python
"format": "json"  # Change from 'parquet' to 'json'
```

Re-run Cell 4 and Cell 8. Verify:
```
Mode: Azure JSON | Container: changefeed-events
Path will be: json/ycsb/public/
```

### **Step 4: Test dual format**
In Cell 4, change:
```python
"format": "both"  # Creates both JSON and Parquet changefeeds
```

Re-run Cell 4 and Cell 8. Verify:
```
Mode: Azure Dual (JSON+Parquet) | Container: changefeed-events
Paths will be:
  - json/ycsb/public/
  - parquet/ycsb/public/
```

---

## ✅ **Verification Checklist**

- [x] Notebook title updated to v2.0
- [x] Breaking changes warning added
- [x] `catalog` parameter added to all modes
- [x] `schema` parameter retained in all modes
- [x] `format` parameter added (direct_mode, azure_parquet_mode)
- [x] Manual `azure_path_prefix` removed
- [x] Auto-constructed path comment added
- [x] Volume mode updated with catalog/schema
- [x] Enhanced output with catalog/schema/format display
- [x] Comments added explaining v2.0 changes

---

## 🚀 **Next Steps**

1. Run the updated notebook to verify all tests pass
2. Test format switching (parquet → json → both)
3. Verify auto-constructed paths in Azure
4. Check that old `azure_path_prefix` is ignored

---

**Status:** ✅ Ready for testing

The notebook is now fully compatible with the v2.0 implementation and will correctly use:
- Required `catalog` and `schema` parameters
- Optional `format` parameter for dual-format support
- Auto-constructed storage paths

**Last Updated:** 2025-12-23 19:30 PST







