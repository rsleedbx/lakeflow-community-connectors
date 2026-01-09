# Critical Fix: Unity Catalog Connections Require channel=preview

## Date
December 18, 2025

## Problem
The CockroachDB connector was not receiving any connection credentials (host, port, database, user, password) from Unity Catalog. Debug output showed only 3 options were being passed:
- `databricks.connection`
- `tablename`
- `tablenamelist`

All other connection parameters were missing, causing the connector to fail with:
```
ValueError: Missing required connection parameters: host, database, user
```

## Investigation
Initially suspected multiple causes:
1. Deployment scripts (copydir.sh, createpipeline.sh)
2. Hardcoded connection name in ingest.py
3. Connection configuration issues

Created `hubspot_copy` (exact copy of hubspot.py) and deployed it using cockroachdb scripts to isolate the issue. Both connectors failed identically, confirming the scripts were fine.

## Root Cause: Missing channel=preview
**The actual issue was that Unity Catalog connections require the `channel=preview` parameter in the DLT pipeline configuration.**

Unity Catalog connection support was a preview feature at the time, and without explicitly enabling the preview channel, the connection parameters were not being passed to the connector.

## ❌ Initial Misdiagnosis
Initially thought the issue was a hardcoded connection name in `ingest.py`:
```python
pipeline_spec = {
    "connection_name": "hubspot_demo",  # This wasn't the real issue
    "objects": [...]
}
```

While this should be dynamic (using the `connection_name` variable), this was **NOT** the root cause of the credential passing issue.

## ✅ Actual Fix
Add `channel=preview` to the DLT pipeline configuration in `createpipeline.sh`:

```python
pipeline_config = {
    "name": f"{catalog}.{schema}.{pipeline_name}",
    "channel": "preview",  # ✅ REQUIRED for Unity Catalog connections!
    "storage": f"{workspace_path}/storage",
    "configuration": {
        "connection_name": connection_name,
        ...
    },
    ...
}
```

## Why channel=preview is Required
Unity Catalog connections with **community connectors** require the preview channel. Without explicitly opting into the preview channel:
- The DLT runtime uses stable channel
- Stable channel doesn't support Unity Catalog connections **for community connectors**
- Connection parameters are not resolved/passed to custom connectors
- Community connector only receives connection name, not actual credentials

With `channel=preview`:
- DLT uses preview runtime
- Preview runtime supports Unity Catalog connections **for community connectors**
- Connection parameters are properly resolved from UC
- Community connector receives all credentials (host, port, database, user, password, etc.)

**Note:** Built-in Databricks connectors may work with UC connections on stable channel. This limitation is specific to community/custom connectors.

## Impact
- **Scripts (copydir.sh, createpipeline.sh)**: ✅ Updated to include `channel=preview`
- **ingest.py**: ✅ Connection name should still be dynamic (not hardcoded)
- **Connector**: ✅ Now receives all connection parameters from Unity Catalog

## Lesson Learned
When debugging **community connector** credential issues with Unity Catalog connections:
1. **First check if `channel=preview` is set in the pipeline configuration**
   - This is required for community/custom connectors using UC connections
   - Built-in connectors may not have this requirement
2. Verify the connection name is dynamic (from config) not hardcoded
3. Ensure the Unity Catalog connection exists and is properly configured
4. Check Databricks documentation for preview feature requirements for community connectors

## Testing
After fix:
- Updated `createpipeline.sh` to include `"channel": "preview"`
- Redeployed using `./copydir.sh`
- Created new pipeline with preview channel
- Connector now receives all connection parameters (token, base_url, host, port, database, user, password, sslmode)

## Files Changed
- `sources/cockroachdb/scripts/createpipeline.sh` - Added `"channel": "preview"`
- `sources/cockroachdb/ingest.py` - Connection name should be dynamic (best practice)

## Reference
- Databricks DLT preview channel documentation
- Unity Catalog connection support in DLT (preview feature)



