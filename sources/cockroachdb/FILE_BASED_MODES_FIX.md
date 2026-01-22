# File-Based Modes Fix (VOLUME + AZURE)

## Issue

When using the connector in file-based modes (VOLUME, AZURE_PARQUET, AZURE_JSON, AZURE_DUAL), calling `get_table_schema()` and `read_table_metadata()` attempted to connect to CockroachDB, causing errors when no credentials were provided.

**Affected Modes**:
- ✅ `ConnectorMode.VOLUME` - Unity Catalog Volumes
- ✅ `ConnectorMode.AZURE_PARQUET` - Azure Blob Storage (Parquet)
- ✅ `ConnectorMode.AZURE_JSON` - Azure Blob Storage (JSON)
- ✅ `ConnectorMode.AZURE_DUAL` - Azure Blob Storage (Both formats)

**Not Affected**:
- ⚠️  `ConnectorMode.DIRECT` - Instream changefeed (needs CockroachDB)

## Root Cause

The methods `get_table_schema()` and `read_table_metadata()` were designed only for `DIRECT` mode where CockroachDB is always available. They always:
1. Listed tables from CockroachDB's `information_schema`
2. Connected to CockroachDB to query schema/metadata
3. Never checked the connector mode

This design assumption broke file-based modes where CockroachDB connection is optional or unavailable.

## Solution

### Unified Fix for All File-Based Modes

Modified both methods to detect file-based modes and read from files instead of querying CockroachDB:

**Before (BROKEN)**:
```python
def get_table_schema(self, table_name: str, table_options: Dict[str, str] = None) -> StructType:
    """Get the Spark schema for a given table."""
    if table_name not in self.list_tables(table_options):  # ❌ Always queries CockroachDB
        raise ValueError(f"Table '{table_name}' not found in schema '{self.schema}'")
    
    conn = self._get_connection(table_options)  # ❌ Always connects
    ...
```

**After (FIXED)**:
```python
def get_table_schema(self, table_name: str, table_options: Dict[str, str] = None) -> StructType:
    """Get the Spark schema for a given table."""
    # In file-based modes, infer schema from files instead of querying CockroachDB
    if self.mode in [ConnectorMode.VOLUME, ConnectorMode.AZURE_PARQUET, ConnectorMode.AZURE_JSON, ConnectorMode.AZURE_DUAL]:
        return self._get_schema_from_files(table_name)  # ✅ Reads from files
    
    # For DIRECT mode, query CockroachDB
    if table_name not in self.list_tables(table_options):
        raise ValueError(f"Table '{table_name}' not found in schema '{self.schema}'")
    
    conn = self._get_connection(table_options)
    ...
```

### New Unified Helper Methods

#### 1. `_get_schema_from_files()`

Infers Spark schema from data files for both VOLUME and AZURE modes:

```python
def _get_schema_from_files(self, table_name: str) -> StructType:
    """
    Get Spark schema by reading from files (Volume or Azure).
    
    Works for:
    - VOLUME mode: Reads from Unity Catalog Volumes
    - AZURE modes: Reads from Azure Blob Storage
    """
    if self.mode == ConnectorMode.VOLUME:
        # List files in volume
        file_list = self._list_volume_files(self.volume_path, spark, dbutils)
        # Read sample file and infer schema
        ...
    else:
        # List blobs in Azure
        blob_list = container_client.list_blobs(name_starts_with=path_prefix)
        # Read sample blob and infer schema
        ...
    
    return df.schema
```

#### 2. `_get_metadata_from_files()`

Reads metadata from `_schema.json` for both VOLUME and AZURE modes:

```python
def _get_metadata_from_files(self, table_name: str) -> Dict[str, Any]:
    """
    Get table metadata by reading from _schema.json file (Volume or Azure).
    
    Returns:
        Dictionary with primary_keys, cursor_field, ingestion_type
    """
    if self.mode == ConnectorMode.VOLUME:
        # Read from volume: /Volumes/.../path/_schema.json
        schema_info = self._load_schema_from_volume(self.volume_path, spark, dbutils)
    else:
        # Read from Azure: {path}/_schema.json
        schema_info = self._load_schema_from_azure(table_name, format_type)
    
    return {
        "primary_keys": schema_info['primary_keys'],
        "cursor_field": "_cdc_updated",
        "ingestion_type": "cdc"
    }
```

## Mode Comparison

| Mode | Data Source | Schema Source | Needs CockroachDB? |
|------|-------------|---------------|-------------------|
| **DIRECT** | Live changefeed | CockroachDB query | ✅ Yes (always) |
| **VOLUME** | Unity Catalog files | Volume files (`_schema.json`) | ❌ No (file-based) |
| **AZURE_PARQUET** | Azure Parquet files | Azure blob (`_schema.json`) | ❌ No (file-based) |
| **AZURE_JSON** | Azure JSON files | Azure blob (`_schema.json`) | ❌ No (file-based) |
| **AZURE_DUAL** | Azure both formats | Azure blob (`_schema.json`) | ❌ No (file-based) |

## Expected Behavior After Fix

### VOLUME Mode (No CockroachDB)
```python
connector = LakeflowConnect({
    'mode': 'volume',
    'volume_path': '/Volumes/.../test-json_usertable_with_split/1769022634',
    'spark': spark,
    'dbutils': dbutils
    # ✅ No CockroachDB credentials needed!
})

# Both methods work without CockroachDB
schema = connector.get_table_schema('usertable', {})
metadata = connector.read_table_metadata('usertable', {})
```

### AZURE Mode (No CockroachDB)
```python
connector = LakeflowConnect({
    'mode': 'azure_parquet',
    'azure_account_name': 'mystorageaccount',
    'azure_account_key': 'key',
    'azure_container': 'changefeed-data',
    'catalog': 'defaultdb',
    'schema': 'public'
    # ✅ No CockroachDB credentials needed!
})

# Both methods work without CockroachDB
schema = connector.get_table_schema('usertable', {})
metadata = connector.read_table_metadata('usertable', {})
```

### DIRECT Mode (Needs CockroachDB)
```python
connector = LakeflowConnect({
    'cockroachdb_url': 'postgresql://user:pass@host:26257/db',
    'catalog': 'defaultdb',
    'schema': 'public'
    # ✅ CockroachDB credentials required for DIRECT mode
})

# Queries CockroachDB (existing behavior)
schema = connector.get_table_schema('usertable', {})
metadata = connector.read_table_metadata('usertable', {})
```

## Benefits

### 1. **Consistent File-Based Operation**
All file-based modes now work the same way:
- No CockroachDB dependency
- Read schema/metadata from files
- Faster (no network latency)

### 2. **Works Offline**
File-based modes can work without CockroachDB availability:
- Testing/development without VPN
- Disaster recovery scenarios
- CI/CD pipelines with pre-synced data

### 3. **Simplified Configuration**
No need to provide CockroachDB credentials when using file-based modes:

**Before (Confusing)**:
```python
# Why do I need CockroachDB creds if I'm reading from Azure? 🤔
connector = LakeflowConnect({
    'mode': 'azure_parquet',
    'azure_account_name': '...',
    'azure_account_key': '...',
    'cockroachdb_url': '...',  # ❌ Shouldn't need this!
    ...
})
```

**After (Clear)**:
```python
# Just provide Azure credentials for AZURE mode
connector = LakeflowConnect({
    'mode': 'azure_parquet',
    'azure_account_name': '...',
    'azure_account_key': '...',
    # ✅ No CockroachDB creds needed!
    ...
})
```

## Files Modified

- ✅ `cockroachdb.py`:
  - Lines 488-498: Modified `get_table_schema()` to check all file-based modes
  - Lines 361-464: Renamed and enhanced `_get_schema_from_volume()` → `_get_schema_from_files()` (supports VOLUME + AZURE)
  - Lines 616-626: Modified `read_table_metadata()` to check all file-based modes
  - Lines 466-521: Renamed and enhanced `_get_metadata_from_volume()` → `_get_metadata_from_files()` (supports VOLUME + AZURE)
- ✅ `FILE_BASED_MODES_FIX.md`: This comprehensive documentation

## Testing Matrix

| Mode | `get_table_schema()` | `read_table_metadata()` | Needs CockroachDB? |
|------|---------------------|------------------------|-------------------|
| DIRECT | ✅ Queries CockroachDB | ✅ Queries CockroachDB | ✅ Yes |
| VOLUME | ✅ Reads from volume files | ✅ Reads from `_schema.json` | ❌ No |
| AZURE_PARQUET | ✅ Reads from Azure blobs | ✅ Reads from `_schema.json` | ❌ No |
| AZURE_JSON | ✅ Reads from Azure blobs | ✅ Reads from `_schema.json` | ❌ No |
| AZURE_DUAL | ✅ Reads from Azure blobs | ✅ Reads from `_schema.json` | ❌ No |

## Related Documentation

- `VOLUME_MODE_SCHEMA_FIX.md` - Original VOLUME-only fix
- `VOLUME_MODE_METADATA_FIX.md` - Original VOLUME-only metadata fix
- `FILE_BASED_MODES_FIX.md` - This file (unified fix for all file-based modes)

## Summary

**Problem**: Only VOLUME mode was fixed; AZURE modes still tried to connect to CockroachDB.  
**Solution**: Extended fix to all file-based modes (VOLUME + AZURE_PARQUET + AZURE_JSON + AZURE_DUAL).  
**Result**: All file-based modes now work without CockroachDB credentials.

**User Impact**: 
- Simplified configuration (no unnecessary CockroachDB credentials)
- Consistent behavior across file-based modes
- Works offline for testing/CI/CD
- Faster schema/metadata retrieval (no network calls)
