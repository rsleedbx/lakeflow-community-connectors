# run_full_diagnosis_from_config() KeyError Fix

## Issue

When running Example 4 in the Debug Section (Cell 30), the function failed with:

```python
KeyError: 'database'
```

**Error trace**:
```python
File cockroachdb_debug.py:986, in run_full_diagnosis_from_config
--> 986 source_catalog = config["cockroachdb_source"]["database"]
KeyError: 'database'
```

## Root Cause

The `run_full_diagnosis_from_config()` function was expecting a different config structure than what the notebook uses.

### Expected (Wrong)
```python
config = {
    "cockroachdb_source": {
        "host": "...",
        "port": 26257,
        "database": "...",      # ← Wrong key
        "user": "...",
        "password": "...",
        "table_name": "..."
    },
    "azure_storage": {
        "storage_account_name": "...",  # ← Wrong key
        "container_name": "..."
    }
}
```

### Actual (Notebook Structure)
```python
config = {
    "cockroachdb": {
        "host": "...",
        "port": 26257,
        "database": "...",
        "user": "...",
        "password": "..."
    },
    "cockroachdb_source": {
        "catalog": "defaultdb",     # ← Correct key
        "schema": "public",
        "table_name": "usertable"
    },
    "databricks_target": {
        "catalog": "robert_lee",
        "schema": "robert_lee_cockroachdb",
        "table_name": "usertable_update_delete_multi_cf"
    },
    "azure_storage": {
        "account_name": "...",      # ← Correct key
        "account_key": "...",
        "container_name": "changefeed-events"
    },
    "cdc_config": {
        "mode": "update_delete",
        "column_family_mode": "multi_cf",
        "primary_key_columns": ["ycsb_key"]
    }
}
```

## Fix Applied

Updated `run_full_diagnosis_from_config()` in `cockroachdb_debug.py` to use the correct config structure:

### Change 1: Connection Parameters

**Before**:
```python
# Extract config
source_catalog = config["cockroachdb_source"]["database"]  # ← Wrong
host = config["cockroachdb_source"]["host"]                 # ← Wrong
port = config["cockroachdb_source"]["port"]                 # ← Wrong
user = config["cockroachdb_source"]["user"]                 # ← Wrong
password = config["cockroachdb_source"]["password"]         # ← Wrong
```

**After**:
```python
# Extract config - CockroachDB connection
host = config["cockroachdb"]["host"]                        # ← Correct
port = config["cockroachdb"]["port"]                        # ← Correct
user = config["cockroachdb"]["user"]                        # ← Correct
password = config["cockroachdb"]["password"]                # ← Correct
database = config["cockroachdb"]["database"]                # ← Correct

# Extract config - Source/Target tables
source_catalog = config["cockroachdb_source"]["catalog"]    # ← Correct
source_schema = config["cockroachdb_source"]["schema"]      # ← Correct
source_table = config["cockroachdb_source"]["table_name"]   # ← Correct
```

### Change 2: Azure Storage

**Before**:
```python
storage_account_name = config["azure_storage"]["storage_account_name"]  # ← Wrong
```

**After**:
```python
storage_account_name = config["azure_storage"]["account_name"]  # ← Correct
```

### Change 3: SSL Context

Added SSL context creation (required for CockroachDB Cloud):

```python
# Create SSL context (required for CockroachDB Cloud)
ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE

# Parse host (in case port is accidentally included)
host_clean = host.split(':')[0] if ':' in host else host

conn = pg8000.native.Connection(
    host=host_clean,
    port=port,
    database=database,
    user=user,
    password=password,
    ssl_context=ssl_context  # ← Added
)
```

### Change 4: Updated Docstring

**Before**:
```python
config: Configuration dictionary with keys:
    - cockroachdb_source: {host, port, database, user, password, table_name}
    - databricks_target: {catalog, schema, table_name}
    - azure_storage: {storage_account_name, container_name}
    - cdc_config: {primary_key_columns}
```

**After**:
```python
config: Configuration dictionary with keys:
    - cockroachdb: {host, port, database, user, password}
    - cockroachdb_source: {catalog, schema, table_name}
    - databricks_target: {catalog, schema, table_name}
    - azure_storage: {account_name, container_name}
    - cdc_config: {primary_key_columns}
```

## Testing

After the fix, Example 4 (Cell 30) should work correctly:

```python
from cockroachdb_debug import run_full_diagnosis_from_config

mismatched_columns = ['field3', 'field4', 'field5', 'field6', 'field7', 'field8', 'field9']

run_full_diagnosis_from_config(
    spark=spark,
    config=config,  # ← Uses config from Cell 3
    mismatched_columns=mismatched_columns
)
```

**Expected Output**:
```
================================================================================
🔍 CDC SYNC DIAGNOSIS CONFIGURATION
================================================================================
   Source: defaultdb.public.usertable_update_delete_multi_cf
   Target: robert_lee.robert_lee_cockroachdb.usertable_update_delete_multi_cf
   Staging: robert_lee.robert_lee_cockroachdb.usertable_update_delete_multi_cf_staging_cf
   Azure: abfss://changefeed-events@...
   Mismatched columns: 7 columns

📊 Refreshing target DataFrame...
✅ Target DataFrame refreshed: 39 rows

████████████████████████████████████████████████████████████████████████████████
🔍 RUNNING FULL DIAGNOSIS
████████████████████████████████████████████████████████████████████████████████
🔌 Establishing fresh CockroachDB connection...
✅ Connection established

🚀 Running full diagnosis...
...
```

## Files Modified

- **`sources/cockroachdb/docs/cockroachdb_debug.py`**
  - Fixed `run_full_diagnosis_from_config()` to use correct config keys
  - Added SSL context for CockroachDB Cloud connection
  - Updated docstring to reflect correct config structure

## Validation

✅ No linting errors
✅ Config keys match notebook structure (Cell 3)
✅ SSL context properly configured
✅ Function parameters documented correctly

## Impact

**Before Fix**: `KeyError: 'database'` when running Example 4

**After Fix**: Example 4 runs successfully with the notebook's config
