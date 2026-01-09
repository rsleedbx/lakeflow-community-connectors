# CDC File Organization - Path-Based Metadata

## 🎯 Overview

The connector uses **path-based organization** to identify catalog, schema, and table information. File paths and names automatically include all metadata needed to identify the source data:

```
{format}/{catalog}/{schema}/{run_timestamp}/{timestamp}-...-{table}-...{ext}
```

Where:
- `{run_timestamp}`: Unix timestamp when the test/changefeed was created (ensures data isolation per run)
- `{timestamp}`: CockroachDB-generated timestamp in the filename

This approach works with standard CockroachDB changefeeds - no custom features needed!

## ✨ Benefits of Path-Based Organization

### Automatic Organization
```
parquet/
  defaultdb/
    public/
      usertable/
        202512...usertable...parquet
        202512...usertable...parquet
    staging/
      orders/
        202512...orders...parquet
  warehouse/
    public/
      inventory/
        202512...inventory...parquet
```

**Key Benefits:**
- ✅ Works with standard CockroachDB (no custom features)
- ✅ Human-readable file paths
- ✅ Easy to browse and organize
- ✅ Storage services can filter by path
- ✅ No post-processing needed

### Reading with Auto-Detection

The connector can still **auto-detect** metadata:

```python
# Minimal configuration - auto-detects from CockroachDB
result = load_and_merge_cdc_to_delta(
    source_table="usertable",  # Used for pathGlobFilter
    volume_path=VOLUME_PATH,
    target_table_path=TARGET_TABLE_PATH,
    crdb_config=CRDB_CONFIG,  # Queries CockroachDB for PK/column families
    catalog="defaultdb",
    schema="public"
)

# Or fully automated - infers from data
result = load_and_merge_cdc_to_delta(
    source_table="usertable",
    volume_path=VOLUME_PATH,
    target_table_path=TARGET_TABLE_PATH
    # No crdb_config needed - infers PK from data
)
```

## 📦 How Path-Based Organization Works

### Changefeed URI Construction

When creating a changefeed, the connector builds the Azure URI:

```python
# From _create_parquet_changefeed() in cockroachdb.py
azure_uri = f"azure://{container}/{parquet_path_prefix}/"

# Where parquet_path_prefix is:
parquet_path_prefix = f"parquet/{catalog}/{schema}"

# Final URI example:
# azure://my-container/parquet/defaultdb/public/
```

CockroachDB then writes files with table name in filename:
```
202512191714242809831900000000000-...-usertable-1.parquet
                                    ^^^^^^^^^
                                    Table name automatically included
```

### Run Timestamp for Data Isolation

**Added Jan 2026**: Each test/changefeed run now includes a unique run timestamp:

```bash
# Generate timestamp when creating changefeed
TEST_RUN_TIMESTAMP=$(date +%s)  # e.g., 1704672000

# Include in path
path="parquet/defaultdb/public/${TEST_RUN_TIMESTAMP}/"
```

**Benefits:**
- ✅ **No stale data**: Each run has isolated directory
- ✅ **Historical analysis**: All test runs preserved
- ✅ **No cleanup needed**: Runs don't interfere with each other
- ✅ **Deterministic tests**: Results always reflect current run only

**Example:**
```
# Test run at 1704672000
parquet/defaultdb/public/1704672000/202512...usertable...parquet

# Test run at 1704672100  
parquet/defaultdb/public/1704672100/202512...usertable...parquet

# Each run has completely isolated data!
```

### Extracting Metadata from Paths

You can parse the path to extract metadata:

```python
# Example path parsing
path = "parquet/defaultdb/public/1704672000/202512...usertable...parquet"
parts = path.split('/')

format = parts[0]         # "parquet"
catalog = parts[1]        # "defaultdb"  
schema = parts[2]         # "public"
run_timestamp = parts[3]  # "1704672000" (when changefeed was created)
filename = parts[4]       # "202512...usertable...parquet"

# Extract table from filename
table = filename.split('-usertable-')[0].split('-')[-1]  # "usertable"

# Convert run_timestamp to datetime if needed
from datetime import datetime
run_time = datetime.fromtimestamp(int(run_timestamp))  # 2024-01-07 12:00:00
```

## 📂 Current Approach: Path-Based Organization (Already Working!)

CockroachDB changefeeds **already include** catalog, schema, and table information in the file paths and names:

### Path Structure
```
azure://container/parquet/defaultdb/public/1704672000/202512...usertable...parquet
                  ^^^^^^  ^^^^^^^^ ^^^^^^  ^^^^^^^^^^      ^^^^^^^^^
                  format  catalog  schema  run_timestamp   table (in filename)
```

This is set automatically by the connector in `_setup_storage_paths()`:
- Path: `{format}/{catalog}/{schema}/{run_timestamp}/`
- `run_timestamp`: Unix timestamp when test/changefeed was created (e.g., `1704672000`)
- Filename: CockroachDB automatically includes table name
- **Benefit**: Each test run has isolated data - no stale data from previous runs

### Example Paths
```
# Parquet format with run timestamp
parquet/defaultdb/public/1704672000/202512191714242809831900000000000-...-usertable-1.parquet

# JSON format with run timestamp
json/defaultdb/public/1704672000/202512191714242809831900000000000-...-usertable-1.ndjson
```

**Path Components:**
- `parquet` or `json` = Format
- `defaultdb` = Catalog/database
- `public` = Schema
- `1704672000` = Run timestamp (when changefeed was created)
- `202512...` = Event timestamp (CockroachDB-generated)
- `usertable` = Table name (in filename)

**This is how we currently identify catalog/schema/table - no custom metadata needed!**

## 🔧 Optional: Add Metadata to Existing Files

If you want to **also** embed metadata inside parquet files for additional validation:

### Option 1: Post-Process with Python

Add metadata to existing parquet files:

```python
import pyarrow.parquet as pq
import pyarrow as pa

def add_metadata_to_parquet(file_path, catalog, schema, table):
    """Add CockroachDB metadata to parquet file."""
    
    # Read existing file
    table_data = pq.read_table(file_path)
    
    # Create new metadata
    existing_metadata = table_data.schema.metadata or {}
    new_metadata = {
        **existing_metadata,
        b'crdb_catalog': catalog.encode('utf-8'),
        b'crdb_schema': schema.encode('utf-8'),
        b'crdb_table': table.encode('utf-8')
    }
    
    # Create new schema with metadata
    new_schema = table_data.schema.with_metadata(new_metadata)
    new_table = table_data.cast(new_schema)
    
    # Write back
    pq.write_table(new_table, file_path)
    print(f"✅ Added metadata to {file_path}")

# Usage
add_metadata_to_parquet(
    '/dbfs/Volumes/main/schema/volume/file.parquet',
    catalog='defaultdb',
    schema='public',
    table='usertable'
)
```

### Option 3: Extract from Path (Recommended)

Instead of adding metadata inside files, parse it from the path:

```python
def extract_metadata_from_path(file_path: str) -> dict:
    """Extract catalog/schema/table/run_timestamp from CDC file path."""
    # Example: "parquet/defaultdb/public/1704672000/202512...usertable...parquet"
    parts = file_path.split('/')
    
    metadata = {}
    if len(parts) >= 5:
        metadata['format'] = parts[0]         # "parquet" or "json"
        metadata['catalog'] = parts[1]        # "defaultdb"
        metadata['schema'] = parts[2]         # "public"
        metadata['run_timestamp'] = parts[3]  # "1704672000"
        
        # Extract table from filename
        filename = parts[4]
        if 'usertable' in filename:  # Simplified - use regex for production
            metadata['table'] = 'usertable'
        
        # Convert timestamp to datetime
        from datetime import datetime
        metadata['run_time'] = datetime.fromtimestamp(int(parts[3]))
    
    return metadata

# Usage
file_path = "parquet/defaultdb/public/1704672000/202512...usertable...parquet"
metadata = extract_metadata_from_path(file_path)
print(metadata)
# {
#   'format': 'parquet', 
#   'catalog': 'defaultdb', 
#   'schema': 'public',
#   'run_timestamp': '1704672000',
#   'table': 'usertable',
#   'run_time': datetime(2024, 1, 7, 12, 0)
# }
```

## 📊 Current Implementation

The `load_and_merge_cdc_to_delta` function now:

1. **Tries to read metadata** from first parquet file
2. **Falls back** to supplied parameters if not found
3. **Shows what it found** in debug output

### Example Output

```
================================================================================
METADATA FROM PARQUET FILES
================================================================================
   catalog: defaultdb
   schema: public
   table: usertable

================================================================================
AUTOMATED CDC TESTING
================================================================================
Source table: usertable
Volume path: dbfs:/Volumes/main/schema/volume
Target table: main.schema.usertable_delta
CockroachDB catalog: defaultdb
CockroachDB schema: public
```

## 🎓 Usage Patterns

### Pattern 1: Fully Self-Documenting (Ideal)

```python
# Parquet files have metadata - no config needed!
result = load_and_merge_cdc_to_delta(
    volume_path=VOLUME_PATH,
    target_table_path=TARGET_TABLE_PATH
)
```

### Pattern 2: Override Metadata

```python
# Use parquet metadata but override catalog
result = load_and_merge_cdc_to_delta(
    volume_path=VOLUME_PATH,
    target_table_path=TARGET_TABLE_PATH,
    catalog="different_db"  # ← Overrides parquet metadata
)
```

### Pattern 3: Fallback for Old Files

```python
# Old files without metadata - supply parameters
result = load_and_merge_cdc_to_delta(
    source_table="usertable",
    volume_path=VOLUME_PATH,
    target_table_path=TARGET_TABLE_PATH,
    catalog="defaultdb",
    schema="public"
)
```

## 🔍 Checking Parquet Metadata

### Using PyArrow

```python
import pyarrow.parquet as pq

# Read metadata
parquet_file = pq.ParquetFile('/dbfs/Volumes/.../file.parquet')
metadata = parquet_file.schema_arrow.metadata

# Print all metadata
if metadata:
    print("Parquet Metadata:")
    for key, value in metadata.items():
        print(f"  {key.decode('utf-8')}: {value.decode('utf-8')}")
else:
    print("No custom metadata found")
```

### Using Databricks Notebook

```python
# Quick check in notebook
import pyarrow.parquet as pq

files = dbutils.fs.ls(VOLUME_PATH)
first_parquet = [f.path for f in files if f.name.endswith('.parquet')][0]

parquet_file = pq.ParquetFile(first_parquet.replace('dbfs:', '/dbfs'))
print("Metadata:", parquet_file.schema_arrow.metadata)
```

## 🚀 Current Status

### ✅ What Works Now

**Path-Based Organization (Standard CockroachDB)**
- Catalog and schema in directory path
- Table name in filename
- No custom CockroachDB features needed
- Works with any blob storage

**Auto-Detection**
- Primary keys: Query CockroachDB or infer from data
- Column families: Query CockroachDB or detect fragmentation
- Table info: From path or supplied parameters

### 🔮 Future Enhancements

If CockroachDB adds custom metadata support in the future:
- Could embed metadata inside parquet files
- Would require CockroachDB enhancement
- Current path-based approach would remain the fallback

## 📝 Example: Add Metadata to Volume

Script to add metadata to all parquet files in a volume:

```python
import pyarrow.parquet as pq
import pyarrow as pa

def add_metadata_to_volume(volume_path, catalog, schema, table):
    """Add metadata to all parquet files in volume."""
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder.getOrCreate()
    files = spark._jvm.com.databricks.dbutils_v1.DBUtilsHolder.dbutils().fs().ls(volume_path)
    
    count = 0
    for f in files:
        if f.name().endswith('.parquet'):
            file_path = f.path().replace('dbfs:', '/dbfs')
            
            try:
                # Read table
                table_data = pq.read_table(file_path)
                
                # Add metadata
                existing_metadata = table_data.schema.metadata or {}
                new_metadata = {
                    **existing_metadata,
                    b'crdb_catalog': catalog.encode('utf-8'),
                    b'crdb_schema': schema.encode('utf-8'),
                    b'crdb_table': table.encode('utf-8')
                }
                
                # Write back
                new_schema = table_data.schema.with_metadata(new_metadata)
                new_table = table_data.cast(new_schema)
                pq.write_table(new_table, file_path)
                
                count += 1
                print(f"✅ {f.name()}")
            except Exception as e:
                print(f"❌ {f.name()}: {e}")
    
    print(f"\n✅ Added metadata to {count} files")

# Usage
add_metadata_to_volume(
    volume_path="dbfs:/Volumes/main/schema/volume/test-scenario",
    catalog="defaultdb",
    schema="public",
    table="usertable"
)
```

## 🎯 Benefits Summary

| Aspect | Value |
|--------|-------|
| **Organization** | Hierarchical by catalog/schema/table |
| **Browsability** | Human-readable paths |
| **Filtering** | Easy to filter by path |
| **Standard** | Works with vanilla CockroachDB |
| **No post-processing** | Automatic organization |

## ✅ Advantages of Path-Based Approach

1. **Standard CockroachDB** - No custom features needed
2. **Human-readable** - Easy to understand file organization
3. **Storage-friendly** - Blob storage can filter/search by path
4. **Tool-compatible** - Works with standard parquet readers
5. **No dependencies** - No PyArrow metadata manipulation needed

## 🔮 Optional Enhancements

If you want additional metadata validation:
1. **Parse paths** - Extract catalog/schema/table from file paths
2. **Embed metadata** - Post-process files with `add_parquet_metadata.py`
3. **Validate consistency** - Verify path matches embedded metadata

## 📚 References

### Related Files
- `cockroachdb.py` - Contains changefeed creation with path-based organization
- `_setup_storage_paths()` - Configures `{format}/{catalog}/{schema}/` paths  
- `_create_parquet_changefeed()` - Creates changefeeds with organized paths
- `test_cdc_matrix.sh` - Tests use production-style hierarchy

### CockroachDB Documentation
- [CREATE CHANGEFEED](https://www.cockroachlabs.com/docs/stable/create-changefeed)
- [Changefeed Sinks](https://www.cockroachlabs.com/docs/stable/changefeed-sinks)
- Automatically includes table name in filenames

### Path Organization
- Format: `{format}/{catalog}/{schema}/{run_timestamp}/{timestamp}-{table}-{shard}.parquet`
- `run_timestamp`: Unix epoch when changefeed/test was created (e.g., `1704672000`)
- `timestamp`: CockroachDB-generated event timestamp in filename
- Set via changefeed `INTO` URI
- Works with Azure, S3, GCS
- **Benefit**: Each test/changefeed run has isolated data directory

---

**Status**: ✅ Using path-based metadata (standard CockroachDB)

**Hierarchy**: `format/catalog/schema/run_timestamp/` structure for organization

**Run Isolation**: Each test/changefeed run uses unique timestamp directory

**Post-processing**: Optional `add_parquet_metadata.py` for embedded metadata

