# Automated CDC Testing Guide

## Overview

The new `load_and_merge_cdc_to_delta()` function automates all CDC testing steps into a single call.

## Features

### Auto-Detection
- ✅ **Primary Keys**: Automatically detected from CockroachDB metadata
- ✅ **Column Families**: Automatically checks if table has `split_column_families`
- ✅ **Fallback**: If CockroachDB unavailable, infers from data schema

### Automated Steps
1. **Load**: Autoloader configuration for incremental processing
2. **Transform**: CDC metadata enrichment (operation, timestamp, source file)
3. **Merge**: Column family fragment deduplication
4. **Write**: Streaming write to Delta table with `complete` output mode
5. **Verify**: Row count validation
6. **Compare**: Source file comparison

## Usage

### Basic Usage

```python
from cockroachdb import load_and_merge_cdc_to_delta, load_crdb_config

# Load credentials
crdb_config = load_crdb_config('.env/cockroachdb_credentials.json')

# Run automated test
result = load_and_merge_cdc_to_delta(
    source_table='usertable',
    volume_path='dbfs:/Volumes/main/schema/volume/test-parquet_usertable_with_split',
    target_table_path='main.schema.usertable_test_delta',
    crdb_config=crdb_config,
    catalog='defaultdb',
    schema='public'
)

# Check results
print(f"Success: {result['success']}")
print(f"Delta rows: {result['delta_count']:,}")
print(f"Source rows: {result['source_count']:,}")
print(f"Match: {result['match']}")
```

### Advanced Usage

```python
# Control individual steps
result = load_and_merge_cdc_to_delta(
    source_table='usertable',
    volume_path='dbfs:/Volumes/...',
    target_table_path='main.schema.table',
    crdb_config=crdb_config,
    catalog='defaultdb',
    schema='public',
    clear_checkpoint=True,      # Clear before processing
    verify=True,               # Verify Delta table
    compare_source=True,       # Compare with source files
    debug=True                 # Show detailed progress
)
```

### Without CockroachDB Connection

```python
# Function will infer primary keys from data
result = load_and_merge_cdc_to_delta(
    source_table='usertable',
    volume_path='dbfs:/Volumes/...',
    target_table_path='main.schema.table',
    crdb_config=None,  # No CockroachDB connection
    debug=True
)
```

## Function Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `source_table` | str | Yes | - | CockroachDB table name |
| `volume_path` | str | Yes | - | Unity Catalog Volume path |
| `target_table_path` | str | Yes | - | Fully qualified Delta table |
| `crdb_config` | dict | No | None | CockroachDB credentials |
| `catalog` | str | No | None | CRDB catalog/database |
| `schema` | str | No | None | CRDB schema |
| `clear_checkpoint` | bool | No | False | Clear checkpoint before processing |
| `verify` | bool | No | True | Verify Delta table after write |
| `compare_source` | bool | No | True | Compare with source files |
| `debug` | bool | No | True | Show detailed progress |

## Return Value

```python
{
    'success': bool,              # Overall success status
    'primary_keys': List[str],    # Detected primary keys
    'has_column_families': bool,  # Whether table has split families
    'delta_count': int,           # Rows in Delta table
    'source_count': int,          # Rows in source files (deduplicated)
    'match': bool,                # Whether counts match
    'query': StreamingQuery       # Spark streaming query object
}
```

## Integration with test_cdc_matrix.sh

### Workflow

```bash
# 1. Run test matrix (auto-syncs to volume)
cd sources/cockroachdb/scripts
./test_cdc_matrix.sh

# 2. Test each scenario in notebook
# - Open: notebooks/test_cdc_scenario.ipynb
# - Update: TEST_SCENARIO = "test-parquet_usertable_with_split"
# - Run all cells
```

### Test Scenarios

| Scenario | Description | Expected Result |
|----------|-------------|-----------------|
| `test-json_usertable_with_split` | JSON, split families | ✅ Merge applied |
| `test-json_usertable_no_split` | JSON, no split | ✅ No merge needed |
| **`test-parquet_usertable_with_split`** | **Parquet, split families** | **✅ Critical test** |
| `test-parquet_usertable_no_split` | Parquet, no split | ✅ No merge needed |
| `test-json_simple_test_no_split` | JSON, simple table | ✅ No merge needed |
| `test-parquet_simple_test_no_split` | Parquet, simple table | ✅ No merge needed |

## Example Output

```
================================================================================
AUTOMATED CDC TESTING
================================================================================
Source table: usertable
Volume path: dbfs:/Volumes/main/robert_lee_cockroachdb/parquet_files/test-parquet_usertable_with_split
Target table: main.robert_lee_cockroachdb.usertable_test_delta

🔍 Auto-detecting table metadata from CockroachDB...
   Primary keys: ['ycsb_key']
   Has column families: True

📥 Loading data with Autoloader...
   ✅ Autoloader configured

🔧 Applying CDC transformations...
   ✅ CDC metadata added

🔀 Merging column family fragments...

🔍 Column Family Merge (Streaming Mode)
   Primary key columns: ['ycsb_key']
   Data columns: 10 columns

🔧 Streaming mode: Applying merge
   (Cannot detect fragmentation in streaming DataFrames)
   - If column families exist: fragments will be merged
   - If no column families: merge is harmless no-op

✅ Merge transformation applied!
   Streaming DataFrame merged
   (Actual counts will be visible after writeStream completes)

💾 Writing to Delta table...
   🚀 Writing to: main.robert_lee_cockroachdb.usertable_test_delta
   ⏳ Processing...
   ✅ Write complete!

✅ Verifying results...
   📊 Delta table: 9,995 rows
      UPSERT: 9,995

📊 Comparing with source files...
   📊 Source files: 11
   📊 Unique keys: 9,995
   📊 UPSERT: 9,995
   📊 DELETE: 0

📊 Comparison:
   Delta: 9,995
   Source: 9,995

   ✅✅✅ PERFECT MATCH! ✅✅✅

================================================================================
TEST COMPLETE
================================================================================
```

## Comparison: Old vs New Approach

### Old Approach (8 cells)

```python
# Cell 1: Setup configuration
import json, os
crdb_config = ...
pipeline_config = ...
SOURCE_TABLE = "usertable"
PRIMARY_KEY_COLUMNS = ["ycsb_key"]  # Manual!
VOLUME_PATH = ...
CHECKPOINT_PATH = ...

# Cell 2: Load with Autoloader
df_raw = spark.readStream.format("cloudFiles")...

# Cell 3: Add CDC metadata
df_enriched = df_raw.withColumn(...)...

# Cell 4: Merge column families
from cockroachdb import merge_column_family_fragments
df_merged = merge_column_family_fragments(...)

# Cell 5: Clear checkpoint (optional)
dbutils.fs.rm(...)

# Cell 6: Write to Delta
query = df_merged.writeStream.toTable(...)
query.awaitTermination()

# Cell 7: Verify results
df_delta = spark.table(...)
delta_count = df_delta.count()

# Cell 8: Compare with source
from cockroachdb import analyze_volume_changefeed_files
stats = analyze_volume_changefeed_files(...)
```

**Issues:**
- ❌ Must manually specify primary keys
- ❌ Must manually check for column families
- ❌ 8 separate cells to run
- ❌ Easy to miss a step
- ❌ Hard to retest multiple scenarios

### New Approach (1 cell!)

```python
# Setup (once)
from cockroachdb import load_and_merge_cdc_to_delta, load_crdb_config
crdb_config = load_crdb_config('.env/cockroachdb_credentials.json')

# Test any scenario (one call)
result = load_and_merge_cdc_to_delta(
    source_table='usertable',
    volume_path='dbfs:/Volumes/.../test-parquet_usertable_with_split',
    target_table_path='main.schema.usertable_test_delta',
    crdb_config=crdb_config,
    catalog='defaultdb',
    schema='public'
)

print(f"Match: {result['match']} ✅" if result['match'] else "⚠️")
```

**Benefits:**
- ✅ Auto-detects primary keys
- ✅ Auto-detects column families
- ✅ 1 function call (vs 8 cells)
- ✅ Built-in validation
- ✅ Easy to test multiple scenarios

## Tips

### Testing Multiple Scenarios

```python
scenarios = [
    'test-json_usertable_with_split',
    'test-parquet_usertable_with_split',
    'test-parquet_usertable_no_split'
]

for scenario in scenarios:
    volume_path = f'dbfs:/Volumes/.../{ scenario}'
    target_table = f'main.schema.usertable_{scenario}_delta'
    
    result = load_and_merge_cdc_to_delta(
        source_table='usertable',
        volume_path=volume_path,
        target_table_path=target_table,
        crdb_config=crdb_config,
        catalog='defaultdb',
        schema='public',
        clear_checkpoint=True
    )
    
    print(f"{scenario}: {result['match']} ✅" if result['match'] else f"{scenario}: ⚠️ FAILED")
```

### Troubleshooting

#### Issue: Primary key detection fails

**Solution:** Either provide CockroachDB config, or ensure data has standard PK column:
```python
# Will try to infer 'ycsb_key', 'id', 'key', or 'pk'
result = load_and_merge_cdc_to_delta(..., crdb_config=None)
```

#### Issue: Count mismatch

**Possible causes:**
1. Checkpoint not cleared → old data mixed with new
2. Wrong volume path → testing different data
3. Merge not applied → 11x inflation for split families

**Solution:**
```python
result = load_and_merge_cdc_to_delta(..., clear_checkpoint=True, debug=True)
# Check debug output to see what was detected
```

## Best Practices

1. **Always test with Parquet + split families first**
   - This is the hardest case (11x fragmentation)
   - If this works, others will too

2. **Use clear_checkpoint=True when testing**
   - Ensures fresh run with known data
   - Avoids mixing old and new data

3. **Keep CockroachDB connection available**
   - Enables accurate primary key detection
   - Detects column families correctly

4. **Run notebook after test_cdc_matrix.sh**
   - Test matrix auto-syncs data to volume
   - Each scenario in its own subdirectory

## References

- Test script: `sources/cockroachdb/scripts/test_cdc_matrix.sh`
- Automated function: `sources/cockroachdb/cockroachdb.py:load_and_merge_cdc_to_delta()`
- Simple notebook: `sources/cockroachdb/notebooks/test_cdc_scenario.ipynb`
- Detailed notebook: `sources/cockroachdb/notebooks/load_parquet_with_merge.ipynb`

