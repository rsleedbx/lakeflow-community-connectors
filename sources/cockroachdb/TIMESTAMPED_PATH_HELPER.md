# Timestamped Path Helper - `get_timestamped_path()`

**Date:** January 8, 2026  
**Status:** ✅ Implemented in `cockroachdb.py`

## Overview

The `get_timestamped_path()` function is a convenience method for working with the timestamp-based path isolation introduced in `test_cdc_matrix.sh` (line 343).

## Background

As of the latest updates, `test_cdc_matrix.sh` creates a unique timestamped directory for each test run to ensure data isolation:

```bash
# Line 343 in test_cdc_matrix.sh
local path_prefix="${format}/${catalog}/${schema}/test-${test_name}/${TEST_RUN_TIMESTAMP}"
```

This creates paths like:
```
json/defaultdb/public/test-json_usertable_with_split/1767823340/
parquet/defaultdb/public/test-parquet_usertable_no_split/1767823500/
```

## Path Convention

```
{volume_base}/{format}/{catalog}/{schema}/test-{test_name}/{timestamp}/
```

**Example:**
```
dbfs:/Volumes/main/schema/volume/json/defaultdb/public/test-json_usertable_with_split/1767823340/
│                                │   │         │      │                                  │
│                                │   │         │      │                                  └─ Unix timestamp
│                                │   │         │      └─ Test scenario name
│                                │   │         └─ Schema name
│                                │   └─ Catalog name
│                                └─ Format (json/parquet)
└─ Volume base path
```

## Function Signature

```python
def get_timestamped_path(
    volume_base: str,
    path_prefix: str,
    version: int = 0,
    dbutils = None
) -> str
```

## Version Parameter

The `version` parameter controls which test run to access:

| Version | Meaning | Example |
|---------|---------|---------|
| `0` | **Latest** (newest timestamp) | Most recent test run |
| `1` | Second newest | Previous test run |
| `2` | Third newest | Two runs ago |
| `-1` | **Oldest** (earliest timestamp) | First test run |
| `-2` | Second oldest | Second test run |

## Usage Examples

### Basic Usage - Get Latest Test Run

```python
from cockroachdb import get_timestamped_path, analyze_volume_changefeed_files

# Get latest test run
latest_path = get_timestamped_path(
    volume_base='dbfs:/Volumes/main/robert_lee_cockroachdb/cdc_test_files',
    path_prefix='json/defaultdb/public/test-json_usertable_with_split',
    version=0,  # Latest (default)
    dbutils=dbutils
)

print(f"Latest test: {latest_path}")
# Output: dbfs:/Volumes/.../test-json_usertable_with_split/1767823340

# Analyze the latest test run
stats = analyze_volume_changefeed_files(
    latest_path,
    primary_key_columns=['ycsb_key'],
    dbutils=dbutils,
    debug=True
)
```

### Compare Multiple Test Runs

```python
from cockroachdb import get_timestamped_path, analyze_volume_changefeed_files

# Get latest and previous test runs
latest = get_timestamped_path(
    volume_base='dbfs:/Volumes/main/robert_lee_cockroachdb/cdc_test_files',
    path_prefix='json/defaultdb/public/test-json_usertable_with_split',
    version=0,
    dbutils=dbutils
)

previous = get_timestamped_path(
    volume_base='dbfs:/Volumes/main/robert_lee_cockroachdb/cdc_test_files',
    path_prefix='json/defaultdb/public/test-json_usertable_with_split',
    version=1,
    dbutils=dbutils
)

# Compare results
latest_stats = analyze_volume_changefeed_files(latest, primary_key_columns=['ycsb_key'], dbutils=dbutils)
previous_stats = analyze_volume_changefeed_files(previous, primary_key_columns=['ycsb_key'], dbutils=dbutils)

print(f"Latest:   {latest_stats['unique_keys']} unique keys")
print(f"Previous: {previous_stats['unique_keys']} unique keys")
```

### Get Oldest Test Run

```python
from cockroachdb import get_timestamped_path

# Get the very first test run
oldest = get_timestamped_path(
    volume_base='dbfs:/Volumes/main/robert_lee_cockroachdb/cdc_test_files',
    path_prefix='json/defaultdb/public/test-json_usertable_with_split',
    version=-1,  # Oldest
    dbutils=dbutils
)

print(f"First test: {oldest}")
```

### Integration with `load_and_merge_cdc_to_delta`

```python
from cockroachdb import get_timestamped_path, load_and_merge_cdc_to_delta, load_crdb_config

# Load credentials
crdb_config = load_crdb_config('.env/cockroachdb_credentials.json')

# Get latest test run path
test_path = get_timestamped_path(
    volume_base='dbfs:/Volumes/main/robert_lee_cockroachdb/cdc_test_files',
    path_prefix='json/defaultdb/public/test-json_usertable_with_split',
    version=0,
    dbutils=dbutils
)

# Load and merge to Delta
result = load_and_merge_cdc_to_delta(
    source_table='usertable',
    volume_path=test_path,
    target_table_path='main.robert_lee_cockroachdb.usertable_test_delta',
    spark=spark,
    dbutils=dbutils,
    crdb_config=crdb_config,
    catalog='defaultdb',
    schema='public',
    clear_checkpoint=True,
    verify=True,
    compare_source=True,
    debug=True
)
```

### Testing Multiple Formats

```python
from cockroachdb import get_timestamped_path, analyze_volume_changefeed_files

volume_base = 'dbfs:/Volumes/main/robert_lee_cockroachdb/cdc_test_files'

# Test both JSON and Parquet versions of the same test
formats = ['json', 'parquet']
for fmt in formats:
    path = get_timestamped_path(
        volume_base=volume_base,
        path_prefix=f'{fmt}/defaultdb/public/test-{fmt}_usertable_with_split',
        version=0,
        dbutils=dbutils
    )
    
    stats = analyze_volume_changefeed_files(
        path,
        primary_key_columns=['ycsb_key'],
        dbutils=dbutils
    )
    
    print(f"{fmt.upper():7} - {stats['unique_keys']:,} unique keys")
```

## Error Handling

### No Timestamped Directories

```python
try:
    path = get_timestamped_path(
        volume_base='dbfs:/Volumes/main/schema/volume',
        path_prefix='json/defaultdb/public/test-nonexistent',
        dbutils=dbutils
    )
except ValueError as e:
    print(f"Error: {e}")
    # Output: No timestamped directories found in: ...
    # Run test_cdc_matrix.sh to generate test data
```

### Version Out of Range

```python
try:
    # Only 3 test runs exist, but requesting 4th
    path = get_timestamped_path(
        volume_base='dbfs:/Volumes/main/schema/volume',
        path_prefix='json/defaultdb/public/test-json_usertable_with_split',
        version=3,  # Doesn't exist
        dbutils=dbutils
    )
except ValueError as e:
    print(f"Error: {e}")
    # Output: Version 3 not found. Only 3 version(s) available.
    # Available versions: 0 to 2 (0=newest, 2=oldest)
```

## Common Patterns

### Get All Available Versions

```python
from cockroachdb import get_timestamped_path

volume_base = 'dbfs:/Volumes/main/robert_lee_cockroachdb/cdc_test_files'
path_prefix = 'json/defaultdb/public/test-json_usertable_with_split'

# Try versions until we get an error
versions = []
version = 0
while True:
    try:
        path = get_timestamped_path(
            volume_base=volume_base,
            path_prefix=path_prefix,
            version=version,
            dbutils=dbutils
        )
        timestamp = path.split('/')[-1]
        versions.append((version, timestamp, path))
        version += 1
    except ValueError:
        break

print(f"Found {len(versions)} test runs:")
for v, ts, p in versions:
    print(f"  Version {v}: {ts}")
```

### Clean Up Old Test Runs

```python
from cockroachdb import get_timestamped_path

# Keep only the latest 3 test runs, delete older ones
volume_base = 'dbfs:/Volumes/main/robert_lee_cockroachdb/cdc_test_files'
path_prefix = 'json/defaultdb/public/test-json_usertable_with_split'

# Start from version 3 (4th newest) onwards
version = 3
while True:
    try:
        old_path = get_timestamped_path(
            volume_base=volume_base,
            path_prefix=path_prefix,
            version=version,
            dbutils=dbutils
        )
        
        print(f"Deleting old test run: {old_path}")
        dbutils.fs.rm(old_path, True)
        version += 1
    except ValueError:
        # No more old versions
        break

print(f"Cleaned up {version - 3} old test runs")
```

## Benefits

1. **Simplified Path Access:** No need to manually construct paths or remember timestamp values
2. **Version Control:** Easy access to historical test runs for comparison
3. **Error Handling:** Clear error messages when paths don't exist or versions are out of range
4. **Convention Enforcement:** Ensures consistent path structure across all test scenarios
5. **Integration Ready:** Works seamlessly with existing analysis and loading functions

## Related Functions

- `analyze_volume_changefeed_files()` - Analyze CDC files in a volume path
- `load_and_merge_cdc_to_delta()` - Load CDC data from volume to Delta table
- `analyze_azure_changefeed_files()` - Similar function for Azure Blob Storage

## Implementation Details

**Location:** `sources/cockroachdb/cockroachdb.py` (line ~4093)

**Key Features:**
- Auto-detects timestamped subdirectories using `dbutils.fs.ls()`
- Validates numeric timestamps (Unix epoch format)
- Sorts by timestamp for correct version ordering
- Provides detailed error messages with suggestions

**Dependencies:**
- `dbutils` - Required for filesystem operations (Databricks utility)
- No other external dependencies

## Migration from Manual Paths

**Before (Manual):**
```python
# Had to manually construct and remember timestamp
volume_path = 'dbfs:/Volumes/main/schema/volume/json/defaultdb/public/test-json_usertable_with_split/1767823340'
stats = analyze_volume_changefeed_files(volume_path, primary_key_columns=['ycsb_key'], dbutils=dbutils)
```

**After (Convenience Method):**
```python
# Automatically gets latest test run
volume_path = get_timestamped_path(
    volume_base='dbfs:/Volumes/main/schema/volume',
    path_prefix='json/defaultdb/public/test-json_usertable_with_split',
    dbutils=dbutils
)
stats = analyze_volume_changefeed_files(volume_path, primary_key_columns=['ycsb_key'], dbutils=dbutils)
```

## Next Steps

This function is ready for use in:
- Test notebooks (`unittest/cockroachdb.ipynb`)
- CDC validation scripts
- Automated testing pipelines
- Data quality checks

**Status:** ✅ Production ready!

