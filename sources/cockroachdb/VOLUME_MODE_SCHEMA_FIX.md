# Volume Mode Schema Inference Fix

## Issue

When using the connector in VOLUME mode, calling `get_table_schema()` attempted to connect to CockroachDB, causing errors when no credentials were provided:

```python
connector = LakeflowConnect({
    'volume_path': '/Volumes/.../test-json_usertable_with_split/1769022634',
    'spark': spark,
    'dbutils': dbutils
})

# ❌ This failed with: InterfaceError: The 'user' connection parameter cannot be None
schema = connector.get_table_schema('usertable', {})
```

**Error:**
```
InterfaceError: The 'user' connection parameter cannot be None
ConnectionError: Failed to connect to CockroachDB: The 'user' connection parameter cannot be None
```

## Root Cause

The `get_table_schema()` method was designed only for direct CockroachDB connections. It always:
1. Listed tables from CockroachDB's `information_schema`
2. Connected to CockroachDB to query column metadata
3. Never checked the connector mode or tried to read from volume files

**Before (BROKEN)**:
```python
def get_table_schema(self, table_name: str, table_options: Dict[str, str] = None) -> StructType:
    """Get the Spark schema for a given table."""
    if table_name not in self.list_tables(table_options):  # ❌ Tries to query CockroachDB
        raise ValueError(f"Table '{table_name}' not found in schema '{self.schema}'")
    
    conn = self._get_connection(table_options)  # ❌ Tries to connect (needs credentials)
    try:
        query = "SELECT ... FROM information_schema.columns ..."  # ❌ Queries database
        ...
```

This design assumed CockroachDB was always available, which is not true for VOLUME mode.

## Solution

Modified `get_table_schema()` to detect VOLUME mode and infer schema from files instead of querying CockroachDB:

**After (FIXED)**:
```python
def get_table_schema(self, table_name: str, table_options: Dict[str, str] = None) -> StructType:
    """Get the Spark schema for a given table."""
    # In VOLUME mode, infer schema from files instead of querying CockroachDB
    if self.mode == ConnectorMode.VOLUME:
        return self._get_schema_from_volume(table_name)
    
    # For other modes, query CockroachDB
    if table_name not in self.list_tables(table_options):
        raise ValueError(f"Table '{table_name}' not found in schema '{self.schema}'")
    
    conn = self._get_connection(table_options)
    ...
```

### New Helper Method: `_get_schema_from_volume()`

```python
def _get_schema_from_volume(self, table_name: str) -> StructType:
    """
    Infer Spark schema from files in Unity Catalog Volume.
    Reads a sample of files to determine the schema.
    """
    spark, dbutils = self._ensure_spark_and_dbutils(self._spark, self._dbutils)
    
    # Get file list from volume
    file_list = self._list_volume_files(self.volume_path, spark=spark, dbutils=dbutils)
    
    if not file_list:
        raise ValueError(f"No files found in volume: {self.volume_path}")
    
    # Filter out metadata files
    data_files = [f for f in file_list if not f['name'].startswith('_')]
    
    if not data_files:
        raise ValueError(f"No data files found in volume: {self.volume_path}")
    
    # Read first file to infer schema
    sample_file = data_files[0]
    is_json = sample_file['name'].endswith(('.ndjson', '.json'))
    
    # Read file and infer schema
    if is_json:
        df = spark.read.json(sample_file['path'])
    else:
        df = spark.read.parquet(sample_file['path'])
    
    # Return inferred schema
    return df.schema
```

## Key Changes

### 1. Mode-Aware Schema Retrieval

The method now branches based on connector mode:
- **VOLUME mode**: Infer from files (no CockroachDB connection needed)
- **DIRECT/AZURE modes**: Query CockroachDB (existing behavior)

### 2. Schema Inference from Files

For VOLUME mode:
1. Lists files in the volume path
2. Filters out metadata files (`_schema.json`, `_checkpoints/`, etc.)
3. Reads the first data file to infer Spark schema
4. Supports both JSON and Parquet formats

### 3. No Credentials Required

VOLUME mode now works without CockroachDB credentials:
```python
# ✅ This now works!
connector = LakeflowConnect({
    'volume_path': '/Volumes/.../data',
    'spark': spark,
    'dbutils': dbutils
    # No CockroachDB credentials needed!
})
schema = connector.get_table_schema('usertable', {})
```

## Expected Behavior After Fix

### Scenario 1: VOLUME Mode (No CockroachDB)
```python
connector = LakeflowConnect({
    'mode': 'volume',
    'volume_path': '/Volumes/main/schema/volume/json/.../test-json_usertable_with_split/1769022634',
    'spark': spark,
    'dbutils': dbutils
})

# ✅ Infers schema from JSON/Parquet files in volume
schema = connector.get_table_schema('usertable', {})
print(f"Schema has {len(schema.fields)} fields")
# Output: Schema has 15 fields (or whatever is in the files)
```

### Scenario 2: DIRECT Mode (With CockroachDB)
```python
connector = LakeflowConnect({
    'cockroachdb_url': 'postgresql://user:pass@host:26257/db',
    'catalog': 'defaultdb',
    'schema': 'public'
})

# ✅ Queries CockroachDB information_schema (existing behavior)
schema = connector.get_table_schema('usertable', {})
```

### Scenario 3: VOLUME Mode WITH CockroachDB Credentials
```python
connector = LakeflowConnect({
    'mode': 'volume',
    'volume_path': '/Volumes/.../data',
    'cockroachdb_url': 'postgresql://...',  # Also provided
    'catalog': 'defaultdb',
    'schema': 'public',
    'spark': spark,
    'dbutils': dbutils
})

# ✅ Uses volume files (doesn't need CockroachDB for schema)
schema = connector.get_table_schema('usertable', {})
```

**Note**: Even with CockroachDB credentials, VOLUME mode prefers to infer from files. This is faster and doesn't require network connectivity.

## Benefits

### 1. **Self-Contained Volume Mode**
No external dependencies - works entirely with files in the volume:
```
/Volumes/main/schema/volume/test-json_usertable/1769022634/
├── _schema.json                    # Metadata (skipped)
├── file1.ndjson                    # ✅ Used for schema inference
├── file2.ndjson
└── file3.ndjson
```

### 2. **Faster Schema Retrieval**
- No network latency (no database connection)
- Reads local files only
- Spark efficiently infers schema from one sample file

### 3. **Works Offline**
Volume mode can work without CockroachDB availability:
- Files synced to volume previously
- Testing/development without VPN
- Disaster recovery scenarios

### 4. **Consistent with Volume Philosophy**
Volume mode is designed for file-based processing. Schema inference from files aligns with this:
- `read_table()` reads from files ✅
- `read_table_metadata()` reads from `_schema.json` ✅
- `get_table_schema()` now reads from files ✅ (was broken before)

## Files Modified

- ✅ `cockroachdb.py`:
  - Lines 421-426: Modified `get_table_schema()` to check mode
  - Lines 361-393: Added new `_get_schema_from_volume()` method
- ✅ `VOLUME_MODE_SCHEMA_FIX.md`: This documentation

## Related Issues

This fix resolves:
1. ✅ `InterfaceError: The 'user' connection parameter cannot be None` in VOLUME mode
2. ✅ Unnecessary CockroachDB dependency for file-based workflows
3. ✅ Confusing behavior where VOLUME mode still tried to connect to database

## Testing

### Test 1: Volume Mode Without Credentials
```python
connector = LakeflowConnect({
    'volume_path': '/Volumes/main/schema/volume/json/.../test-json_usertable_with_split/1769022634',
    'spark': spark,
    'dbutils': dbutils
})

# Should succeed
schema = connector.get_table_schema('usertable', {})
assert len(schema.fields) > 0
print("✅ Schema inferred from volume files")
```

### Test 2: Direct Mode (Existing Behavior)
```python
connector = LakeflowConnect({
    'cockroachdb_url': 'postgresql://user:pass@host:26257/db',
    'catalog': 'defaultdb',
    'schema': 'public'
})

# Should query CockroachDB
schema = connector.get_table_schema('test_table', {})
assert len(schema.fields) > 0
print("✅ Schema queried from CockroachDB")
```

### Test 3: Empty Volume
```python
connector = LakeflowConnect({
    'volume_path': '/Volumes/empty/path',
    'spark': spark,
    'dbutils': dbutils
})

# Should fail with clear error
try:
    schema = connector.get_table_schema('usertable', {})
except ValueError as e:
    assert "No files found in volume" in str(e)
    print("✅ Clear error message for empty volume")
```

## Summary

**Problem**: `get_table_schema()` always tried to connect to CockroachDB, breaking VOLUME mode.  
**Solution**: Detect VOLUME mode and infer schema from files instead.  
**Result**: VOLUME mode is now fully self-contained and doesn't require CockroachDB credentials.

**User Impact**: Users can now call `get_table_schema()` in VOLUME mode without providing CockroachDB credentials, making the connector more flexible and easier to use for file-based workflows.
