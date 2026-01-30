# cockroachdb_azure.py

Azure Blob Storage utilities for CockroachDB CDC changefeeds.

---

## Functions

### 1. `check_azure_files()`

Check for changefeed files in Azure Blob Storage.

**Signature**:
```python
def check_azure_files(
    storage_account_name: str,
    storage_account_key: str,
    container_name: str,
    source_catalog: str,
    source_schema: str,
    source_table: str,
    target_table: str,
    verbose: bool = True
) -> Dict[str, Any]
```

**Parameters**:
- `storage_account_name`: Azure storage account name
- `storage_account_key`: Azure storage account access key
- `container_name`: Azure Blob Storage container name
- `source_catalog`: CockroachDB catalog (database name)
- `source_schema`: CockroachDB schema (e.g., "public")
- `source_table`: Source table name in CockroachDB
- `target_table`: Target table name (used in path)
- `verbose`: Print detailed output (default: True)

**Returns**:
```python
{
    'data_files': [BlobProperties, ...],     # Parquet data files
    'resolved_files': [BlobProperties, ...], # .RESOLVED watermark files
    'total': int                              # Total blob count
}
```

**Path Format**:
```
parquet/{source_catalog}/{source_schema}/{source_table}/{target_table}/
```

---

### 2. `wait_for_changefeed_files()`

Wait for changefeed files to appear in Azure with timeout and stabilization period.

**Signature**:
```python
def wait_for_changefeed_files(
    storage_account_name: str,
    storage_account_key: str,
    container_name: str,
    source_catalog: str,
    source_schema: str,
    source_table: str,
    target_table: str,
    max_wait: int = 120,
    check_interval: int = 5,
    stabilization_wait: int = 5
) -> bool
```

**Parameters**:
- `storage_account_name`: Azure storage account name
- `storage_account_key`: Azure storage account access key
- `container_name`: Azure Blob Storage container name
- `source_catalog`: CockroachDB catalog (database name)
- `source_schema`: CockroachDB schema (e.g., "public")
- `source_table`: Source table name in CockroachDB
- `target_table`: Target table name (used in path)
- `max_wait`: Maximum seconds to wait (default: 120)
- `check_interval`: Seconds between checks (default: 5)
- `stabilization_wait`: Seconds to wait for file count to stabilize (default: 5)

**Returns**:
- `True`: Files found and stabilized
- `False`: Timeout occurred

**Behavior**:
1. Polls Azure every `check_interval` seconds
2. When first file appears, switches to stabilization mode
3. Waits for file count to stabilize for `stabilization_wait` seconds
4. Returns `True` when stable, or `False` if max_wait exceeded

---

## Usage Examples

### Check for CDC Files

```python
from cockroachdb_azure import check_azure_files

result = check_azure_files(
    storage_account_name="mystorageaccount",
    storage_account_key="<access-key>",
    container_name="cockroachcdc",
    source_catalog="defaultdb",
    source_schema="public",
    source_table="usertable",
    target_table="usertable_append_only_multi_cf",
    verbose=True
)

print(f"Found {len(result['data_files'])} data files")
print(f"Found {len(result['resolved_files'])} resolved files")

# Access file details
if result['data_files']:
    first_file = result['data_files'][0]
    print(f"First file: {first_file.name}")
    print(f"Size: {first_file.size} bytes")
```

**Output**:
```
📁 Files in Azure changefeed path:
   Path: parquet/defaultdb/public/usertable/usertable_append_only_multi_cf/
   📄 Data files: 12
   🕐 Resolved files: 4
   📊 Total: 16

   Example data file:
   parquet/defaultdb/public/usertable/usertable_append_only_multi_cf/202601301234567890.0.parquet

Found 12 data files
Found 4 resolved files
```

---

### Wait for Files (After Changefeed Creation)

```python
from cockroachdb_azure import wait_for_changefeed_files

# Create changefeed (in CockroachDB)
# ...

# Wait for initial snapshot files to appear
success = wait_for_changefeed_files(
    storage_account_name="mystorageaccount",
    storage_account_key="<access-key>",
    container_name="cockroachcdc",
    source_catalog="defaultdb",
    source_schema="public",
    source_table="usertable",
    target_table="usertable_append_only_multi_cf",
    max_wait=300,
    check_interval=5,
    stabilization_wait=10  # Wait 10s for column family fragments
)

if success:
    print("✅ Files are ready for Databricks ingestion!")
else:
    print("⚠️  Timeout - check changefeed status")
```

**Output** (column family mode):
```
⏳ Waiting for initial snapshot files to appear in Azure...

✅ First files appeared after 15 seconds!
   Found 3 file(s) so far...
   Waiting 10s for more files (column family fragments)...
   📄 File count increased: 3 → 6
   📄 File count increased: 6 → 9

✅ File count stable at 9 for 10s
   Total wait time: 35s
   Example: parquet/defaultdb/public/usertable/usertable_append_only_multi_cf/202601301234567890.0.parquet

✅ Files are ready for Databricks ingestion!
```

---

## In Databricks Notebook

### Setup

```python
# Cell 1: Configuration
storage_account_name = "mystorageaccount"
storage_account_key = dbutils.secrets.get("azure", "storage-key")
container_name = "cockroachcdc"

source_catalog = "defaultdb"
source_schema = "public"
source_table = "usertable"
target_table = "usertable_append_only_multi_cf"

# Cell 2: Import
from cockroachdb_azure import check_azure_files, wait_for_changefeed_files
```

### Check Files Before Ingestion

```python
# Cell 3: Verify files exist
result = check_azure_files(
    storage_account_name, storage_account_key, container_name,
    source_catalog, source_schema, source_table, target_table
)

if len(result['data_files']) == 0:
    raise Exception("No CDC files found! Check changefeed status.")
else:
    print(f"✅ Ready to ingest {len(result['data_files'])} files")
```

---

## File Filtering Logic

The `check_azure_files()` function filters files using the same logic as `cockroachdb.py`:

**Included** (data_files):
- ✅ Files ending with `.parquet`
- ✅ In the correct changefeed path
- ❌ Excluding `.RESOLVED` files (watermarks)
- ❌ Excluding `_metadata/` directory (schema info)
- ❌ Excluding files starting with `_` (_SUCCESS, _committed_*, etc.)

**Included** (resolved_files):
- ✅ Files containing `.RESOLVED` in name

---

## Column Family Handling

When using `split_column_families` in CockroachDB changefeeds, multiple files are written per timestamp:

```
usertable/202601301234567890.0.parquet           # Primary key + default family
usertable/202601301234567890.1.parquet           # frequently_read family
usertable/202601301234567890.2.parquet           # medium_read family
usertable/202601301234567890.3.parquet           # rarely_read family
```

The `wait_for_changefeed_files()` function handles this by:
1. Waiting for first file
2. Continuing to wait for more files
3. Exiting when file count stabilizes (no new files for N seconds)

**Recommended Settings**:
- `stabilization_wait=5` for single family
- `stabilization_wait=10` for multi-family (3-4 families)

---

## Error Handling

### Connection Errors

```python
from cockroachdb_azure import check_azure_files

try:
    result = check_azure_files(
        storage_account_name="mystorageaccount",
        storage_account_key="wrong-key",
        container_name="cockroachcdc",
        source_catalog="defaultdb",
        source_schema="public",
        source_table="usertable",
        target_table="usertable_cdc"
    )
except Exception as e:
    print(f"❌ Azure connection failed: {e}")
    # Example: Server failed to authenticate the request
```

### Empty Results

```python
result = check_azure_files(
    storage_account_name, storage_account_key, container_name,
    source_catalog, source_schema, source_table, target_table,
    verbose=False
)

if len(result['data_files']) == 0:
    print("⚠️  No files found - possible causes:")
    print("   1. Changefeed not created yet")
    print("   2. Changefeed paused or failed")
    print("   3. Incorrect path parameters")
    print("   4. Files in different container or path")
```

---

## Dependencies

```python
from azure.storage.blob import BlobServiceClient
import time
from typing import Dict, Any, List
```

Install dependencies:
```bash
pip install azure-storage-blob
```

Or in Databricks:
```python
%pip install azure-storage-blob --quiet
```

---

## Related Modules

- `cockroachdb_conn.py` - CockroachDB connection management
- `cockroachdb_autoload.py` - CDC ingestion functions (4 modes)
- `cockroachdb_debug.py` - CDC diagnosis utilities
- `cockroachdb_ycsb.py` - YCSB test data generation

---

## Path Structure

### CockroachDB Changefeed Configuration

```sql
CREATE CHANGEFEED FOR TABLE usertable
INTO 'azure://cockroachcdc/parquet/defaultdb/public/usertable/usertable_cdc?AZURE_ACCOUNT_NAME=mystorageaccount&AZURE_ACCOUNT_KEY=<key>'
WITH format='parquet', updated, resolved='10s';
```

### Resulting Azure Path

```
Container: cockroachcdc
Path:      parquet/defaultdb/public/usertable/usertable_cdc/
Files:     
  - 202601301234567890.0.parquet
  - 202601301234567890.1.parquet
  - 202601301235567890.0.RESOLVED
  - ...
```

### Module Path Matching

```python
check_azure_files(
    storage_account_name="mystorageaccount",
    storage_account_key="<key>",
    container_name="cockroachcdc",
    source_catalog="defaultdb",      # Matches: parquet/defaultdb/
    source_schema="public",          # Matches: .../public/
    source_table="usertable",        # Matches: .../usertable/
    target_table="usertable_cdc"     # Matches: .../usertable_cdc/
)
```

---

Date: 2026-01-30
