# Notebook Simplification - Auto-Extract Table Name ✅

**Date:** January 7, 2026  
**File:** `test_cdc_scenario.ipynb`  
**Status:** ✅ **COMPLETE - Only TEST_SCENARIO required!**

---

## 🎯 **Goal**

Simplify notebook configuration to require **only ONE variable** (`TEST_SCENARIO`), with all other parameters auto-derived.

---

## ✅ **What Changed**

### **Before (4 variables required):**

```python
# 🔧 CHANGE THESE TO TEST DIFFERENT SCENARIOS
TEST_SCENARIO = "test-parquet_simple_test_no_split"  # ⭐ Change this!

# OPTIONAL Source table in CockroachDB
CRDB_CATALOG = "defaultdb"  # CockroachDB database/catalog
CRDB_SCHEMA = "public"      # CockroachDB schema
SOURCE_TABLE = "simple_test"  # "usertable" or "simple_test"
```

### **After (1 variable required):**

```python
# 🔧 CHANGE THIS TO TEST DIFFERENT SCENARIOS
TEST_SCENARIO = "test-parquet_simple_test_no_split"  # ⭐ Change this!

# Auto-extract table name from TEST_SCENARIO
# Pattern: test-{format}_{table_name}_{split_word1}_{split_word2}
# Examples:
#   "test-parquet_simple_test_no_split" → "simple_test"
#   "test-json_usertable_with_split" → "usertable"
scenario_parts = TEST_SCENARIO.split('_')
if len(scenario_parts) >= 4:
    # Remove "test-{format}" (first part) and split info (last 2 parts)
    # Middle parts = table name (can be single or multi-word)
    SOURCE_TABLE = '_'.join(scenario_parts[1:-2])
else:
    raise ValueError(...)

# CockroachDB connection defaults (used for schema auto-detection)
# Override these only if your test data uses a different catalog/schema
CRDB_CATALOG = "defaultdb"  # CockroachDB database/catalog
CRDB_SCHEMA = "public"      # CockroachDB schema
```

---

## 📋 **Parsing Logic**

### **Pattern:**
```
test-{format}_{table_name}_{split_word1}_{split_word2}
```

### **Algorithm:**
1. Split scenario by `_`
2. Remove first part: `test-{format}` (contains the dash)
3. Remove last 2 parts: `{split_word1}_{split_word2}` (e.g., `no_split`, `with_split`)
4. Join remaining middle parts: **table name** (can be single or multi-word)

### **Examples:**

| TEST_SCENARIO | Split Parts | Middle Parts | SOURCE_TABLE |
|---------------|-------------|--------------|--------------|
| `test-parquet_simple_test_no_split` | `['test-parquet', 'simple', 'test', 'no', 'split']` | `['simple', 'test']` | `simple_test` ✅ |
| `test-json_usertable_with_split` | `['test-json', 'usertable', 'with', 'split']` | `['usertable']` | `usertable` ✅ |
| `test-parquet_usertable_no_split` | `['test-parquet', 'usertable', 'no', 'split']` | `['usertable']` | `usertable` ✅ |

---

## ✅ **Verification Tests**

```bash
$ python3 test_parsing_logic.py

🧪 Testing table name extraction:
================================================================================
✅ test-json_usertable_with_split
   Expected: usertable
   Extracted: usertable

✅ test-json_usertable_no_split
   Expected: usertable
   Extracted: usertable

✅ test-parquet_usertable_with_split
   Expected: usertable
   Extracted: usertable

✅ test-parquet_usertable_no_split
   Expected: usertable
   Extracted: usertable

✅ test-json_simple_test_no_split
   Expected: simple_test
   Extracted: simple_test

✅ test-parquet_simple_test_no_split
   Expected: simple_test
   Extracted: simple_test

================================================================================
✅ ALL TESTS PASSED!
```

---

## 🎯 **Benefits**

### **1. Simpler Configuration**
- **Before:** Change 4 variables (TEST_SCENARIO, SOURCE_TABLE, CRDB_CATALOG, CRDB_SCHEMA)
- **After:** Change 1 variable (TEST_SCENARIO) ✅

### **2. No Manual Synchronization**
- **Before:** Must manually keep `SOURCE_TABLE` in sync with `TEST_SCENARIO`
- **After:** Automatic extraction - impossible to get out of sync ✅

### **3. Clear Defaults**
- **Before:** `CRDB_CATALOG` and `CRDB_SCHEMA` looked required
- **After:** Clearly marked as defaults with override instructions ✅

### **4. Better Error Messages**
- **Before:** Silent mismatch if table name wrong
- **After:** Clear validation error if TEST_SCENARIO format invalid ✅

---

## 📖 **Usage**

### **Quick Start (Most Common):**

```python
# Just change this one line!
TEST_SCENARIO = "test-parquet_usertable_with_split"  # ⭐

# Everything else is automatic:
# ✅ SOURCE_TABLE = "usertable" (auto-extracted)
# ✅ CRDB_CATALOG = "defaultdb" (default)
# ✅ CRDB_SCHEMA = "public" (default)
```

### **Advanced (Custom Catalog/Schema):**

```python
TEST_SCENARIO = "test-parquet_usertable_with_split"

# Override defaults if needed:
CRDB_CATALOG = "production_db"  # Custom catalog
CRDB_SCHEMA = "staging"         # Custom schema

# SOURCE_TABLE still auto-extracted ✅
```

---

## 🔍 **Output Example**

When you run the cell, you'll see:

```
✅ Auto-detected table: simple_test
   Using catalog: defaultdb
   Using schema: public

================================================================================
TEST CONFIGURATION
================================================================================
Test scenario: test-parquet_simple_test_no_split
Source table: simple_test
Volume path: dbfs:/Volumes/main/robert_lee_cockroachdb/parquet_files/parquet/defaultdb/public/test-parquet_simple_test_no_split
Target table: main.robert_lee_cockroachdb.simple_test_test_parquet_simple_test_no_split_delta
================================================================================
```

---

## 🎓 **Design Principles**

### **1. Convention over Configuration**
- Use naming conventions to infer configuration
- Reduces manual input and errors

### **2. Smart Defaults**
- Most common values (`defaultdb`, `public`) are defaults
- Override only when needed

### **3. Fail Fast**
- Invalid TEST_SCENARIO format → Clear error message immediately
- No silent failures

### **4. Self-Documenting**
- Pattern shown in comments with examples
- Output confirms what was detected

---

## 🚀 **Impact**

### **Before:**
```python
# ❌ Error-prone: Must keep these in sync manually
TEST_SCENARIO = "test-parquet_simple_test_no_split"
SOURCE_TABLE = "usertable"  # ❌ WRONG! Should be "simple_test"
```

### **After:**
```python
# ✅ Impossible to get out of sync
TEST_SCENARIO = "test-parquet_simple_test_no_split"
# SOURCE_TABLE automatically extracted as "simple_test" ✅
```

---

## 📝 **Summary**

| Aspect | Before | After |
|--------|--------|-------|
| **Variables to change** | 4 (TEST_SCENARIO, SOURCE_TABLE, CRDB_CATALOG, CRDB_SCHEMA) | 1 (TEST_SCENARIO) ✅ |
| **Risk of mismatch** | High (manual sync) | Zero (automatic) ✅ |
| **User experience** | Confusing (which to change?) | Simple (change one thing) ✅ |
| **Error handling** | Silent failures | Clear validation ✅ |
| **Defaults** | Implicit | Explicit with override docs ✅ |

---

## ✅ **Conclusion**

**Configuration simplified from 4 variables → 1 variable!**

**User Experience:**
- ✅ **Change one line** (`TEST_SCENARIO`)
- ✅ **Everything else automatic** (table name, paths, targets)
- ✅ **Clear feedback** (shows what was detected)
- ✅ **Easy to override** (defaults clearly marked)

**Code Quality:**
- ✅ **DRY principle** (don't repeat table name)
- ✅ **Fail fast** (invalid format → immediate error)
- ✅ **Self-documenting** (pattern explained with examples)
- ✅ **Tested** (all 6 scenarios verified)

**Philosophy:** **Make the simple things simple, and the complex things possible!** 🎯

---

**Date:** January 7, 2026  
**Status:** ✅ **COMPLETE - Tested with all 6 scenarios**


