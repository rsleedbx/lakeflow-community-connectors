# Volume Mode Metadata Fix

## Issue

When using the connector in VOLUME mode, calling `read_table_metadata()` attempted to connect to CockroachDB, causing errors:

```python
connector = LakeflowConnect({
    'volume_path': '/Volumes/.../test-json_usertable_with_split/1769022634',
    'spark': spark,
    'dbutils': dbutils
})

# ❌ This failed with: ValueError: Table 'usertable' not found in schema 'public'
metadata = connector.read_table_metadata('usertable', {})
```

**Error:**
```
ValueError: Table 'usertable' not found in schema 'public'
```

This happened because the method tried to call `self.list_tables()`, which queries CockroachDB's `information_schema`, but no CockroachDB connection was available in VOLUME mode.

## Root Cause

The `read_table_metadata()` method was designed only for direct CockroachDB connections. It always:
1. Listed tables from CockroachDB's `information_schema`
2. Connected to CockroachDB to query primary keys
3. Never checked the connector mode or tried to read from volume files

**Before (BROKEN)**:
```python
def read_table_metadata(self, table_name: str, table_options: Dict[str, str]) -> Dict[str, Any]:
    """Read table metadata."""
    if table_name not in self.list_tables(table_options):  # ❌ Tries to query CockroachDB
        raise ValueError(f"Table '{table_name}' not found in schema '{self.schema}'")
    
    conn = self._get_connection(table_options)  # ❌ Tries to connect (needs credentials)
    try:
        pk_query = "SELECT ... FROM information_schema ..."  # ❌ Queries database
        ...
```

This design assumed CockroachDB was always available, which is not true for VOLUME mode.

## Solution

Modified `read_table_metadata()` to detect VOLUME mode and read metadata from `_schema.json` file instead of querying CockroachDB:

**After (FIXED)**:
```python
def read_table_metadata(self, table_name: str, table_options: Dict[str, str]) -> Dict[str, Any]:
    """Read table metadata."""
    # In VOLUME mode, read metadata from _schema.json file instead of querying CockroachDB
    if self.mode == ConnectorMode.VOLUME:
        return self._get_metadata_from_volume(table_name)
    
    # For other modes, query CockroachDB
    if table_name not in self.list_tables(table_options):
        raise ValueError(f"Table '{table_name}' not found in schema '{self.schema}'")
    
    conn = self._get_connection(table_options)
    ...
```

### New Helper Method: `_get_metadata_from_volume()`

```python
def _get_metadata_from_volume(self, table_name: str) -> Dict[str, Any]:
    """
    Get table metadata by reading from Unity Catalog Volume _schema.json file.
    
    Returns:
        Dictionary with primary_keys, cursor_field, ingestion_type
    """
    spark, dbutils = self._ensure_spark_and_dbutils(self._spark, self._dbutils)
    
    # Load schema metadata from _schema.json
    schema_info = self._load_schema_from_volume(self.volume_path, spark=spark, dbutils=dbutils)
    
    if not schema_info:
        raise ValueError(
            f"Schema file not found in volume: {self.volume_path}/_schema.json\n"
            f"Run test_cdc_matrix.sh to generate schema files."
        )
    
    primary_keys = schema_info.get('primary_keys', [])
    
    if not primary_keys:
        raise ValueError(
            f"No primary keys found in schema file: {self.volume_path}/_schema.json\n"
            f"Ensure the table has a primary key defined."
        )
    
    return {
        "primary_keys": primary_keys,
        "cursor_field": "_cdc_updated",
        "ingestion_type": "cdc"
    }
```

## Key Changes

### 1. Mode-Aware Metadata Retrieval

The method now branches based on connector mode:
- **VOLUME mode**: Read from `_schema.json` file (no CockroachDB connection needed)
- **DIRECT/AZURE modes**: Query CockroachDB (existing behavior)

### 2. Reuses Existing `_load_schema_from_volume()`

The new helper method leverages the existing `_load_schema_from_volume()` method:
- No code duplication
- Consistent error handling
- Uses same schema file format

### 3. No Credentials Required

VOLUME mode now works without CockroachDB credentials:
```python
# ✅ This now works!
connector = LakeflowConnect({
    'volume_path': '/Volumes/.../data/1769022634',
    'spark': spark,
    'dbutils': dbutils
    # No CockroachDB credentials needed!
})
metadata = connector.read_table_metadata('usertable', {})
print(metadata['primary_keys'])  # ['ycsb_key']
```

## Expected Behavior After Fix

### Scenario 1: VOLUME Mode (No CockroachDB)
```python
connector = LakeflowConnect({
    'mode': 'volume',
    'volume_path': '/Volumes/.../test-json_usertable_with_split/1769022634',
    'spark': spark,
    'dbutils': dbutils
})

# ✅ Reads metadata from _schema.json file in volume
metadata = connector.read_table_metadata('usertable', {})
print(metadata['primary_keys'])  # ['ycsb_key']
print(metadata['ingestion_type'])  # 'cdc'
```

### Scenario 2: DIRECT Mode (With CockroachDB)
```python
connector = LakeflowConnect({
    'cockroachdb_url': 'postgresql://user:pass@host:26257/db',
    'catalog': 'defaultdb',
    'schema': 'public'
})

# ✅ Queries CockroachDB information_schema (existing behavior)
metadata = connector.read_table_metadata('usertable', {})
```

### Scenario 3: VOLUME Mode WITHOUT _schema.json
```python
connector = LakeflowConnect({
    'volume_path': '/Volumes/.../data_without_schema_file/1769022634',
    'spark': spark,
    'dbutils': dbutils
})

# ❌ Clear error message
try:
    metadata = connector.read_table_metadata('usertable', {})
except ValueError as e:
    print(e)
    # "Schema file not found in volume: .../data/_schema.json
    #  Run test_cdc_matrix.sh to generate schema files."
```

## Benefits

### 1. **Self-Contained Volume Mode**
No external dependencies - works entirely with files in the volume:
```
/Volumes/.../test-json_usertable/1769022634/
├── _schema.json                    # ✅ Used for metadata
├── file1.ndjson                    # ✅ Used for data
├── file2.ndjson
└── file3.ndjson
```

### 2. **Faster Metadata Retrieval**
- No network latency (no database connection)
- Reads small JSON file (~1KB)
- Much faster than database query

### 3. **Works Offline**
Volume mode can work without CockroachDB availability:
- Files synced to volume previously
- Testing/development without VPN
- Disaster recovery scenarios

### 4. **Consistent with Volume Philosophy**
All volume operations now read from files:
- `read_table()` reads from data files ✅
- `get_table_schema()` infers from data files ✅ (from previous fix)
- `read_table_metadata()` reads from `_schema.json` ✅ (this fix)

## Relationship to Other Fixes

This fix complements the `get_table_schema()` fix from `VOLUME_MODE_SCHEMA_FIX.md`:

| Method | Previous Behavior | New Behavior (VOLUME mode) |
|--------|-------------------|----------------------------|
| `get_table_schema()` | Queried CockroachDB ❌ | Infers from data files ✅ |
| `read_table_metadata()` | Queried CockroachDB ❌ | Reads from `_schema.json` ✅ |
| `read_table()` | Already worked ✅ | Still works ✅ |

Now **all three methods** work in VOLUME mode without CockroachDB credentials!

## Files Modified

- ✅ `cockroachdb.py`:
  - Lines 579-589: Modified `read_table_metadata()` to check mode
  - Lines 397-427: Added new `_get_metadata_from_volume()` method
- ✅ `VOLUME_MODE_METADATA_FIX.md`: This documentation

## Testing

### Test 1: Volume Mode Without Credentials
```python
connector = LakeflowConnect({
    'volume_path': '/Volumes/.../test-json_usertable_with_split/1769022634',
    'spark': spark,
    'dbutils': dbutils
})

# Should succeed
metadata = connector.read_table_metadata('usertable', {})
assert 'primary_keys' in metadata
assert 'ycsb_key' in metadata['primary_keys']
print("✅ Metadata read from volume _schema.json")
```

### Test 2: Direct Mode (Existing Behavior)
```python
connector = LakeflowConnect({
    'cockroachdb_url': 'postgresql://user:pass@host:26257/db',
    'catalog': 'defaultdb',
    'schema': 'public'
})

# Should query CockroachDB
metadata = connector.read_table_metadata('test_table', {})
assert len(metadata['primary_keys']) > 0
print("✅ Metadata queried from CockroachDB")
```

### Test 3: Volume Without Schema File
```python
connector = LakeflowConnect({
    'volume_path': '/Volumes/empty/path/without/schema/1769022634',
    'spark': spark,
    'dbutils': dbutils
})

# Should fail with clear error
try:
    metadata = connector.read_table_metadata('usertable', {})
except ValueError as e:
    assert "Schema file not found" in str(e)
    assert "test_cdc_matrix.sh" in str(e)
    print("✅ Clear error message for missing schema file")
```

## Complete VOLUME Mode Flow

After both fixes (`get_table_schema()` and `read_table_metadata()`), the complete flow works without CockroachDB:

```python
# Initialize connector (no CockroachDB credentials)
connector = LakeflowConnect({
    'volume_path': '/Volumes/.../test-json_usertable_with_split/1769022634',
    'spark': spark,
    'dbutils': dbutils
})

# 1. Get Spark schema from data files
schema = connector.get_table_schema('usertable', {})
print(f"✅ Schema: {len(schema.fields)} columns")

# 2. Get metadata from _schema.json
metadata = connector.read_table_metadata('usertable', {})
print(f"✅ Primary keys: {metadata['primary_keys']}")

# 3. Read data using iterator pattern
for batch, offset in connector.read_table('usertable', {'cursor': ''}, {}):
    print(f"✅ Read batch: {len(batch)} records")
    if not batch:
        break

print("🎉 Complete VOLUME mode workflow - no CockroachDB needed!")
```

## Summary

**Problem**: `read_table_metadata()` always tried to connect to CockroachDB, breaking VOLUME mode.  
**Solution**: Detect VOLUME mode and read metadata from `_schema.json` file instead.  
**Result**: VOLUME mode is now fully self-contained for all metadata operations.

**User Impact**: Users can now call both `get_table_schema()` and `read_table_metadata()` in VOLUME mode without providing CockroachDB credentials, making the connector truly file-based for offline/testing scenarios.
