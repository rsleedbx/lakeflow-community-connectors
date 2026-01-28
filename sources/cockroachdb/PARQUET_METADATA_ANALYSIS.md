# CockroachDB Changefeed: Parquet Metadata Analysis

**Date**: January 28, 2026  
**Status**: ⚠️ **Primary key and schema metadata NOT included in production Parquet files**

---

## 🔍 Summary

CockroachDB changefeed Parquet files **do NOT include** primary key or schema metadata inside the Parquet file metadata. This information is only available:
1. **In test builds** (via custom metadata)
2. **In file paths** (catalog/schema/table names)
3. **In filenames** (table name only)

---

## 📊 What Metadata IS Included

### Standard Columns (Always Present)

| Column | Type | Description |
|--------|------|-------------|
| `<user_columns>` | Various | All table columns (data) |
| `__crdb__event_type` | STRING | Event type: `c` (snapshot/change), `i` (insert), `d` (delete) |

### Optional Metadata Columns

| Column | When Included | Type | Changefeed Option |
|--------|---------------|------|-------------------|
| `__crdb__updated` | If `updated` option set | STRING | `updated` |
| `__crdb__mvcc_timestamp` | If `mvcc_timestamp` option set | STRING | `mvcc_timestamp` |
| `__crdb__before` | If `diff` option set | JSON | `diff` |

### Parquet File Metadata (Production Builds)

**None** - Production Parquet files contain **no custom metadata** about:
- ❌ Primary key columns
- ❌ Primary key column positions
- ❌ Table name
- ❌ Database/catalog name
- ❌ Schema name
- ❌ Column family information

---

## 🧪 What Metadata IS Included (Test Builds Only)

### Test Build Configuration

Test metadata is **only** enabled if:
```go
// From pkg/ccl/changefeedccl/parquet.go line 31
var includeParquestTestMetadata = buildutil.CrdbTestBuild ||
    envutil.EnvOrDefaultBool("COCKROACH_CHANGEFEED_TESTING_INCLUDE_PARQUET_TEST_METADATA", false)
```

### Test Metadata Contents

When enabled, Parquet file metadata includes:

| Metadata Key | Content | Example |
|-------------|---------|---------|
| `keyCols` | Primary key column names and positions | `"ycsb_key,0"` |
| `allCols` | All column names and positions | `"ycsb_key,0,field0,1,field1,2,..."` |

**Format**: Comma-separated list of `columnName,position` pairs

**Example**:
```
keyCols:  "id,0,tenant_id,1"
allCols:  "id,0,tenant_id,1,name,2,email,3,created_at,4,__crdb__event_type,5,__crdb__updated,6"
```

### Code Reference

**File**: `pkg/ccl/changefeedccl/parquet.go` (lines 238-327)

```go
// addParquetTestMetadata appends options to configure the parquet writer
// to write metadata required by cdc test feed factories.
func addParquetTestMetadata(...) {
    // ... builds keyCols and allCols maps ...
    
    parquetOpts = append(parquetOpts, 
        parquet.WithMetadata(map[string]string{
            "keyCols": serializeMap(keysInOrder, keyCols)
        }))
    parquetOpts = append(parquetOpts, 
        parquet.WithMetadata(map[string]string{
            "allCols": serializeMap(valuesInOrder, valueCols)
        }))
    return parquetOpts, nil
}
```

---

## 📂 Alternative: Path-Based Metadata

Since Parquet files don't contain schema metadata, CockroachDB uses **path-based organization**:

### Path Structure

```
azure://container/{format}/{catalog}/{schema}/{run_timestamp}/...{table}-1.parquet
                   ^^^^^^  ^^^^^^^^ ^^^^^^  ^^^^^^^^^^          ^^^^^
                   format  catalog  schema  run_timestamp       table
```

### Example

```
parquet/defaultdb/public/1704672000/202512191714242809831900000000000-6af85e13912c5e8d-1-44-00000000-usertable-1.parquet
^^^^^^  ^^^^^^^^  ^^^^^^  ^^^^^^^^^^                                                                      ^^^^^^^^^
format  catalog   schema  run_timestamp                                                                   table
```

### Extracting Metadata from Paths

```python
# Example path
path = "parquet/defaultdb/public/1704672000/202512...usertable-1.parquet"
parts = path.split('/')

format_type = parts[0]     # "parquet"
catalog = parts[1]         # "defaultdb"
schema = parts[2]          # "public"
run_timestamp = parts[3]   # "1704672000" (when changefeed was created)
filename = parts[4]        # "202512...usertable-1.parquet"

# Extract table from filename (appears before "-1.parquet")
# Format: {timestamp}-{jobid}-{shard}-{node}-{sequence}-{table}-{version}.parquet
table = filename.rsplit('-', 2)[0].split('-')[-1]  # "usertable"
```

---

## 🚨 Missing Primary Key Information

### The Problem

Without primary key metadata in Parquet files, downstream consumers must:

1. **Query CockroachDB** to get primary key columns:
   ```sql
   SELECT column_name 
   FROM information_schema.key_column_usage 
   WHERE table_name = 'usertable' 
   AND constraint_name = 'usertable_pkey'
   ORDER BY ordinal_position;
   ```

2. **Hardcode** primary keys in application logic:
   ```python
   # Must be manually specified
   primary_keys = ['ycsb_key']
   ```

3. **Parse from external schema registry** (if available)

### Impact on CDC Consumers

**Databricks Auto Loader / Spark**:
- ❌ Cannot automatically determine which columns to use for MERGE operations
- ❌ Must manually specify primary keys in code
- ❌ Schema evolution doesn't include PK changes

**AWS Glue / EMR**:
- ❌ Cannot auto-generate deduplication logic
- ❌ Must maintain external PK mapping

**Azure Synapse**:
- ❌ Cannot auto-create clustered indexes on correct columns

---

## 🔧 Workarounds

### Option 1: Query CockroachDB for Primary Keys

```python
def get_primary_keys(conn, table_name):
    """Query CockroachDB for primary key columns."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT column_name 
        FROM information_schema.key_column_usage 
        WHERE table_name = %s 
        AND constraint_name LIKE '%%_pkey'
        ORDER BY ordinal_position
    """, (table_name,))
    return [row[0] for row in cursor.fetchall()]
```

### Option 2: Use Test Metadata in Development

Enable test metadata for development/testing:

```bash
export COCKROACH_CHANGEFEED_TESTING_INCLUDE_PARQUET_TEST_METADATA=true
```

Then read metadata from Parquet files:

```python
import pyarrow.parquet as pq

def read_primary_keys_from_metadata(parquet_file):
    """Read primary keys from test metadata."""
    metadata = pq.read_metadata(parquet_file)
    file_metadata = metadata.metadata
    
    if b'keyCols' in file_metadata:
        key_cols_str = file_metadata[b'keyCols'].decode('utf-8')
        # Parse "ycsb_key,0" -> ["ycsb_key"]
        parts = key_cols_str.split(',')
        return [parts[i] for i in range(0, len(parts), 2)]
    else:
        raise ValueError("No keyCols metadata found (production build?)")
```

### Option 3: Post-Process Parquet Files

Add custom metadata after files are written:

```python
import pyarrow.parquet as pq
import pyarrow as pa

def add_primary_key_metadata(file_path, primary_keys):
    """Add primary key metadata to existing Parquet file."""
    table = pq.read_table(file_path)
    
    # Create new metadata
    metadata = {
        b'primary_keys': ','.join(primary_keys).encode('utf-8')
    }
    
    # Merge with existing metadata
    existing_metadata = table.schema.metadata or {}
    existing_metadata.update(metadata)
    
    # Write back with new metadata
    new_schema = table.schema.with_metadata(existing_metadata)
    new_table = table.cast(new_schema)
    pq.write_table(new_table, file_path)
```

### Option 4: Maintain External Schema Registry

Create a separate metadata file:

```json
{
  "defaultdb.public.usertable": {
    "primary_keys": ["ycsb_key"],
    "columns": {
      "ycsb_key": "INT",
      "field0": "STRING",
      "field1": "STRING"
    }
  }
}
```

---

## 💡 Feature Request Potential

### Should Primary Keys Be in Parquet Metadata?

**Arguments FOR**:
- ✅ Self-describing files (no external lookups needed)
- ✅ Enables auto-MERGE in Spark/Databricks
- ✅ Consistent with Kafka Connect, Debezium, Airbyte (all include PK info)
- ✅ Already implemented for test builds
- ✅ Minimal overhead (a few bytes per file)

**Arguments AGAINST**:
- ❌ Increases file size (negligible - ~100 bytes)
- ❌ Breaks backward compatibility (if consumers expect no metadata)
- ❌ Not part of Parquet standard (custom metadata is fine per spec)

### Comparison with Other CDC Tools

| Tool | Primary Key Metadata | Format |
|------|---------------------|--------|
| Debezium | ✅ YES | In message schema |
| Kafka Connect | ✅ YES | In Avro/JSON schema |
| Airbyte | ✅ YES | In destination connector config |
| Fivetran | ✅ YES | Inferred from source |
| **CockroachDB** | ❌ **NO** (production) | N/A |

---

## 🎯 Current State (January 2026)

### What Works

✅ Data columns are correctly typed and named  
✅ CDC event types (`__crdb__event_type`) work correctly  
✅ Timestamps (`__crdb__updated`) work correctly  
✅ Path-based organization (catalog/schema/table) works  
✅ Test metadata works in development

### What Doesn't Work

❌ No primary key metadata in production Parquet files  
❌ No table name metadata in production Parquet files  
❌ No schema/catalog metadata in production Parquet files  
❌ Cannot auto-detect PK columns for MERGE operations  
❌ Cannot validate PK constraints in downstream systems

### Recommendation

**For Production Use**:
1. Query CockroachDB for primary keys at setup time
2. Store PK mapping in external configuration
3. Use path-based metadata (catalog/schema/table) for organization

**For Development/Testing**:
1. Enable test metadata via env var
2. Read PK info from Parquet metadata
3. Validate against actual schema

**For Future Enhancement**:
- Consider filing a feature request to include primary key metadata in production builds
- Propose minimal metadata format: `{"primary_keys": ["col1", "col2"]}`
- Reference existing test metadata implementation as proof of concept

---

## 📚 Code References

### Primary Key Metadata (Test Only)

**File**: `pkg/ccl/changefeedccl/parquet.go`

- Lines 29-33: Test metadata configuration
- Lines 123-127: Test metadata inclusion logic
- Lines 238-327: `addParquetTestMetadata()` function
- Lines 324-325: Metadata serialization

### Schema Definition

**File**: `pkg/ccl/changefeedccl/parquet.go`

- Lines 48-86: `newParquetSchemaDefintion()` - builds schema from row
- Lines 92-108: `appendMetadataColsToSchema()` - adds CDC columns
- Lines 58-64: Column deduplication (PK columns mentioned but not tracked)

### Filename Generation

**File**: Not directly in parquet.go - handled by sink layer

Expected format:
```
{timestamp}-{jobid}-{shard}-{node}-{sequence}-{table}-{version}.parquet
```

---

## 🔗 Related Issues

- Issue #161962: `.RESOLVED` files use DECIMAL(2147483647, 0) breaking Spark
- Path-based metadata documented in:
  - `MULTI_CATALOG_SCHEMA_SUPPORT.md`
  - `CDC_FILE_ORGANIZATION.md`
  - `PARQUET_METADATA_GUIDE.md`
