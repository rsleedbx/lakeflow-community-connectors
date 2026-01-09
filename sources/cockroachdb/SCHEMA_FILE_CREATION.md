# Schema File Creation for Test Matrix

## Overview

The test matrix (`test_cdc_matrix.sh`) now automatically creates `_schema.json` files alongside changefeed data. This ensures test data has the same structure as production changefeeds created via the Python API.

## What Changed

### 1. New Command in `changefeed_helper.py`

Added `create-schema-file` command:

```bash
python3 changefeed_helper.py create-schema-file \
  --table simple_test \
  --json crdb_creds.json \
  --account <azure_account> \
  --key <azure_key> \
  --container changefeed-events \
  --prefix parquet/defaultdb/public/test-parquet_simple_test_no_split/1767823340
```

**What it does:**
1. Connects to CockroachDB to get table metadata:
   - Primary keys
   - Column definitions (name, type, nullable)
   - CREATE TABLE statement
   - Column families detection
2. Generates a schema JSON file
3. Uploads to Azure at `{prefix}/_schema.json`

**Schema structure:**
```json
{
  "table_name": "simple_test",
  "catalog": "defaultdb",
  "schema": "public",
  "primary_keys": ["id"],
  "columns": [
    {"name": "id", "type": "INT8", "nullable": false},
    {"name": "name", "type": "STRING", "nullable": true},
    ...
  ],
  "create_statement": "CREATE TABLE ...",
  "has_column_families": false,
  "schema_version": 1,
  "created_at": "2026-01-08T12:34:56Z"
}
```

### 2. Updated `test_cdc_matrix.sh`

Added schema file creation after changefeed creation (line 683):

```bash
# Create schema file
echo "📄 Creating schema file..."
local schema_output=$(python3 "$SCRIPTS_DIR/changefeed_helper.py" create-schema-file \
    --table "$table" \
    --json "$CRDB_JSON" \
    --account "${azure_creds[azure_account]}" \
    --key "${azure_creds[azure_key]}" \
    --container "changefeed-events" \
    --prefix "$path_prefix" 2>&1)

if echo "$schema_output" | grep -q "✅ Schema file created"; then
    echo "$schema_output"
else
    echo -e "${YELLOW}⚠️  Failed to create schema file (non-fatal)${NC}"
    echo "$schema_output"
fi
```

**Behavior:**
- Creates schema file immediately after changefeed creation
- Non-fatal: Test continues even if schema creation fails
- Schema file is stored in the timestamped directory

## Benefits

### ✅ Eliminates Warning Message

**Before:**
```
🔍 Loading schema from volume...
   ⚠️  Failed to load schema from volume: file not found
   
🔍 Fallback: Querying CockroachDB for metadata...
   ✅ Got metadata from CockroachDB
```

**After:**
```
🔍 Loading schema from volume...
   ✅ Schema loaded from volume
   Primary keys: ['id']
```

### ✅ Test Data Matches Production

- Test data now has identical structure to production changefeeds
- Schema files are self-documenting (include primary keys, column families info)
- No need for CockroachDB connection when loading test data

### ✅ Faster Testing

- Schema is cached in Azure alongside data
- No need to query CockroachDB when analyzing files
- Validation mode works without CockroachDB access

## File Locations

For a test run with timestamp `1767823340`:

```
changefeed-events/
└── parquet/defaultdb/public/test-parquet_simple_test_no_split/1767823340/
    ├── _schema.json  ← NEW!
    ├── 202601070010040888179060000000000-da7e571c8f43967a-1-30600-00000000-test_parquet_simple_test_no_split-1.parquet
    └── 202601070010326285842180000000001-da7e571c8f43967a-1-30600-00000001-test_parquet_simple_test_no_split-1.parquet
```

## Testing

Run the test matrix:

```bash
cd sources/cockroachdb/scripts
./test_cdc_matrix.sh parquet
```

You'll now see:
```
🚀 Creating changefeed...
✅ Changefeed created: Job 12345

📄 Creating schema file...
✅ Schema file created: changefeed-events/parquet/defaultdb/public/test-parquet_simple_test_no_split/1767823340/_schema.json
   Primary keys: ['id']
   Columns: 3
   Column families: False
```

## Backward Compatibility

- Existing test data without schema files still works (CockroachDB fallback)
- New runs automatically get schema files
- No changes needed to notebooks or existing workflows

## Implementation Details

**New function in `changefeed_helper.py`:**
- `cmd_create_schema_file()` - Handles the new command
- Uses same logic as `LakeflowConnect._dump_table_schema()`
- Directly uploads to Azure Blob Storage

**Modified function in `test_cdc_matrix.sh`:**
- `run_test()` - Calls schema creation after changefeed creation
- Non-blocking: Test continues even if schema creation fails

