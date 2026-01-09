# Schema Management for CockroachDB Changefeeds

## Overview

The CockroachDB connector now automatically dumps and stores table schema information alongside changefeed data files. This enables production-ready primary key detection and eliminates the need for naming pattern heuristics.

## How It Works

### 1. **Automatic Schema Dump on Changefeed Creation**

When a changefeed is created, the connector automatically:
1. Extracts complete schema from CockroachDB
2. Stores it as `_schema.json` alongside data files
3. Makes it available for all data processing operations

```python
# When you create a changefeed:
connector = create_connector(crdb_config, azure_config=azure_config)
job_id = connector.create_changefeed_to_azure(
    table_name='usertable',
    azure_uri='azure://...',
    changefeed_format='parquet'
)

# Automatically creates:
# azure://container/parquet/defaultdb/public/usertable/_schema.json
```

### 2. **Schema File Structure**

The `_schema.json` file contains:

```json
{
  "table_name": "usertable",
  "catalog": "defaultdb",
  "schema": "public",
  "primary_keys": ["ycsb_key"],
  "columns": [
    {
      "name": "ycsb_key",
      "type": "STRING",
      "nullable": false
    },
    {
      "name": "field0",
      "type": "STRING",
      "nullable": true
    }
  ],
  "create_statement": "CREATE TABLE public.usertable (...)",
  "has_column_families": true,
  "schema_version": 1,
  "created_at": "2025-01-06T10:30:00Z"
}
```

### 3. **Storage Locations**

#### Azure Blob Storage

```
azure://container/
├── parquet/defaultdb/public/
│   └── usertable/
│       ├── _schema.json  ← Schema file
│       ├── 202512...parquet  ← Data files
│       └── 202512...parquet
└── json/defaultdb/public/
    └── usertable/
        ├── _schema.json  ← Schema file
        ├── 202512...ndjson  ← Data files
        └── 202512...ndjson
```

#### Unity Catalog Volume

```
/Volumes/main/schema/volume/
├── _schema.json  ← Schema file
├── 202512...parquet  ← Data files
└── 202512...parquet
```

## Usage

### Automatic (Recommended)

The schema is automatically loaded and used - **no code changes needed**!

```python
# Schema is automatically loaded when reading data
connector = LakeflowConnect(options)
data, offset = connector.read_table('usertable', start_offset={}, {})

# Primary keys are automatically used for deduplication
```

### Manual Schema Access

You can also manually access schema information:

```python
# Load schema from Azure
schema_info = connector._load_schema_from_azure('usertable', 'parquet')
print(f"Primary keys: {schema_info['primary_keys']}")
print(f"Has column families: {schema_info['has_column_families']}")

# Load schema from Volume
schema_info = connector._load_schema_from_volume('dbfs:/Volumes/.../volume')
```

## Benefits

### ✅ Production-Ready

- **Works for any table** - No dependency on naming conventions
- **Handles complex PKs** - Composite primary keys, custom names, etc.
- **Self-documenting** - Data includes its own schema

### ✅ Performance

- **No runtime queries** - Schema loaded once from storage
- **Efficient deduplication** - Uses only PK columns (not all columns)
- **Reduced network** - No CockroachDB connection needed during processing

### ✅ Reliability

- **Schema versioning** - Track schema changes over time
- **Audit trail** - Know exactly what schema was active when
- **Graceful degradation** - Falls back to inference if schema unavailable

## Schema File Requirement

Schema files are **REQUIRED** for all CDC operations:

1. **Mandatory** - All changefeeds must have an accompanying `_schema.json` file
2. **Auto-generated** - Schema files are automatically created when using `cockroachdb.py` to create changefeeds
3. **Clear errors** - If schema file is missing, functions will fail with clear error messages

**Benefits of this approach:**
- **Simpler code** - No complex fallback logic
- **Reliable** - Always use correct primary keys from source table
- **Production-ready** - Enforces best practices from day one

## Implementation Details

### When Schema is Dumped

Schema is automatically dumped when:
- Creating a new Parquet changefeed
- Creating a new JSON changefeed
- Using `create_changefeed_to_azure()`

### When Schema is Loaded

Schema is loaded (REQUIRED) when:
- Reading from Azure Parquet mode
- Reading from Unity Catalog Volume
- Analyzing changefeed files

### Error Handling

If schema file is missing:

```python
# Clear error with actionable fix:
FileNotFoundError: Schema file not found: path/_schema.json
Schema file is required for CDC analysis.
Create changefeeds using cockroachdb.py to automatically generate schema files.
```

## Testing

The schema management is thoroughly tested by:

```bash
cd sources/cockroachdb/scripts
./test_cdc_matrix.sh
```

This tests:
- Schema dump on changefeed creation
- Schema load during data processing
- Primary key extraction and usage
- Fallback behavior when schema missing

## Troubleshooting

### Schema File Not Found

**Symptom**: Warning message "Schema file not found"

**Solution**: This is normal for old changefeeds. Options:
1. Manually dump schema (see Migration above)
2. Recreate changefeed to auto-dump schema
3. Continue using inference (works but less efficient)

### Incorrect Primary Keys

**Symptom**: Deduplication not working correctly

**Solution**: 
1. Check schema file exists and is valid:
   ```bash
   azure storage blob download \
     --account-name $ACCOUNT \
     --container-name $CONTAINER \
     --name "parquet/defaultdb/public/usertable/_schema.json"
   ```

2. Verify primary keys are correct:
   ```sql
   -- In CockroachDB:
   SHOW COLUMNS FROM usertable;
   ```

3. Regenerate schema if needed (see Migration)

### Schema Version Mismatch

**Symptom**: Schema doesn't match current table structure

**Solution**: Dump fresh schema after schema changes:
```python
# After ALTER TABLE:
schema_info = connector._dump_table_schema('usertable')
connector._store_schema_to_azure('usertable', schema_info, 'parquet')
```

## Future Enhancements

Potential future improvements:

1. **Schema evolution tracking** - Store multiple versions
2. **Automatic schema refresh** - Detect and update on DDL changes
3. **Schema validation** - Verify data matches schema
4. **Cross-format sharing** - Reuse schema across Parquet/JSON

## Related Files

- `cockroachdb.py` - Contains schema dump/load implementation
- `_dump_table_schema()` - Extracts schema from CockroachDB
- `_store_schema_to_azure()` - Saves schema to Azure
- `_load_schema_from_azure()` - Loads schema from Azure
- `_store_schema_to_volume()` - Saves schema to Volume
- `_load_schema_from_volume()` - Loads schema from Volume
- `_process_parquet_records()` - Uses schema for PK extraction

## References

- [CockroachDB CREATE CHANGEFEED](https://www.cockroachlabs.com/docs/stable/create-changefeed)
- [Parquet File Format](https://parquet.apache.org/docs/)
- [Unity Catalog Volumes](https://docs.databricks.com/en/connect/unity-catalog/volumes.html)

---

**Status**: ✅ Implemented and tested

**Version**: 1.0

**Last Updated**: 2025-01-06

