# Azure Path Test Plan: CockroachDB Changefeed

## Problem

CockroachDB changefeed is configured with:
```
azure://container/parquet/db/schema/table/target_table/?...
```

But writes files to:
```
parquet/db/schema/table/
```

**Missing**: The final `/{target_table}/` component is being stripped!

---

## Test 1: Try `path_prefix` Query Parameter (S3-style)

Even though documentation says Azure doesn't support it, let's test if newer versions do:

### Current Approach (Not Working)
```python
path = f"parquet/{source_catalog}/{source_schema}/{source_table}/{target_table}"
changefeed_path = f"azure://{container_name}/{path}?AZURE_ACCOUNT_NAME=...&AZURE_ACCOUNT_KEY=..."
```

### Test Approach 1A: `path_prefix` Parameter
```python
base_path = f"azure://{container_name}/"
path_prefix = f"parquet/{source_catalog}/{source_schema}/{source_table}/{target_table}"
changefeed_path = f"{base_path}?path_prefix={path_prefix}&AZURE_ACCOUNT_NAME=...&AZURE_ACCOUNT_KEY=..."
```

**Expected**: CockroachDB might honor `path_prefix` for Azure like it does for S3

---

## Test 2: Shallower Path Structure

Maybe CockroachDB has a maximum path depth for Azure URIs?

### Test Approach 2A: Only 2 Levels Deep
```python
# Instead of: parquet/db/schema/table/target_table (5 levels)
# Try:        parquet/table_target                 (2 levels)
path = f"parquet/{source_table}_{target_table}"
changefeed_path = f"azure://{container_name}/{path}?AZURE_ACCOUNT_NAME=...&AZURE_ACCOUNT_KEY=..."
```

### Test Approach 2B: Only 3 Levels Deep
```python
# Try: parquet/schema/table_target (3 levels)
path = f"parquet/{source_schema}/{source_table}_{target_table}"
changefeed_path = f"azure://{container_name}/{path}?AZURE_ACCOUNT_NAME=...&AZURE_ACCOUNT_KEY=..."
```

---

## Test 3: Use Container Name as Path Component

Maybe the issue is with Azure-specific URI parsing?

### Test Approach 3A: Dedicated Container per Target
```python
# Instead of: azure://container/parquet/db/schema/table/target/
# Try:        azure://container-target/parquet/db/schema/table/
custom_container = f"{container_name}_{target_table}"
path = f"parquet/{source_catalog}/{source_schema}/{source_table}"
changefeed_path = f"azure://{custom_container}/{path}?AZURE_ACCOUNT_NAME=...&AZURE_ACCOUNT_KEY=..."
```

**Note**: Would require creating multiple Azure containers

---

## Test 4: Check CockroachDB Version

CockroachDB versions might have different Azure path handling:

```sql
SELECT version();
```

Check if there are known Azure path bugs in your version:
- https://github.com/cockroachdb/cockroach/issues

---

## Test 5: Use S3-Compatible Storage

As a workaround, use Azure Data Lake Gen2 with S3-compatible access:

```python
# Azure Data Lake Gen2 supports S3 API
s3_compatible_path = f"s3://bucket/path?..."
# With path_prefix query parameter
```

---

## Recommended Test Sequence

### Step 1: Quick Test - `path_prefix` Parameter

```python
# In notebook Cell 8, replace:
path = f"parquet/{source_catalog}/{source_schema}/{source_table}/{target_table}"
changefeed_path = f"azure://{container_name}/{path}?AZURE_ACCOUNT_NAME={storage_account_name}&AZURE_ACCOUNT_KEY={storage_account_key_encoded}"

# With:
base_container = f"azure://{container_name}/"
path_prefix = f"parquet/{source_catalog}/{source_schema}/{source_table}/{target_table}"
changefeed_path = f"{base_container}?path_prefix={path_prefix}&AZURE_ACCOUNT_NAME={storage_account_name}&AZURE_ACCOUNT_KEY={storage_account_key_encoded}"
```

**Test**:
1. Drop existing changefeed
2. Create with new URI format
3. Wait for files
4. Check Azure path: `parquet/defaultdb/public/usertable_append_only_multi_cf/usertable_append_only_multi_cf/`
5. If files appear with full path → SUCCESS!

### Step 2: If Test 1 Fails - Test Path Depth

```python
# Try flatter structure
path = f"parquet_{source_table}_{target_table}"
changefeed_path = f"azure://{container_name}/{path}?AZURE_ACCOUNT_NAME={storage_account_name}&AZURE_ACCOUNT_KEY={storage_account_key_encoded}"
```

Update Auto Loader to match:
```python
source_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/parquet_{source_table}_{target_table}"
```

### Step 3: If All Tests Fail - File CockroachDB Bug

Create issue at: https://github.com/cockroachdb/cockroach/issues/new

**Title**: Azure changefeed ignores nested path components in URI

**Body**:
```
## Summary
CockroachDB changefeed configured with Azure URI containing nested paths 
only writes to a subset of the path, ignoring the deepest components.

## Expected Behavior
Changefeed URI:
`azure://container/parquet/db/schema/table/target/?...`

Should write files to:
`parquet/db/schema/table/target/`

## Actual Behavior
Files are written to:
`parquet/db/schema/table/`

The final `/target/` component is stripped.

## CockroachDB Version
[Your version from SELECT version()]

## Changefeed SQL
CREATE CHANGEFEED FOR TABLE usertable_append_only_multi_cf
INTO 'azure://container/parquet/defaultdb/public/usertable_append_only_multi_cf/usertable_append_only_multi_cf/?AZURE_ACCOUNT_NAME=...&AZURE_ACCOUNT_KEY=...'
WITH format='parquet', updated, resolved='10s', split_column_families;

## Impact
Prevents multiple Databricks targets from ingesting from the same source table,
as all targets would process the same CDC files.

## Workaround Request
1. Support `path_prefix` query parameter for Azure (like S3)
2. Or document maximum path depth for Azure URIs
3. Or fix path handling to honor all components
```

---

## Alternative Solution: Use Azure Path as-is

If CockroachDB consistently strips the last component, we can:

1. **Accept the behavior** and adjust our design:
   ```python
   # Don't use /{target_table}/ for path isolation
   # Instead use Databricks-side filtering
   ```

2. **Use Auto Loader's `pathGlobFilter`** to isolate by timestamp or other metadata:
   ```python
   raw_df = (spark.readStream
       .format("cloudFiles")
       .option("pathGlobFilter", "*2026-01-30*")  # Timestamp-based isolation
       .load(source_path)
   )
   ```

3. **Use separate Azure containers** for each target:
   ```python
   # Production: container=prod-cdc
   # Staging:    container=staging-cdc  
   # Analytics:  container=analytics-cdc
   ```

---

## Date

2026-01-30
