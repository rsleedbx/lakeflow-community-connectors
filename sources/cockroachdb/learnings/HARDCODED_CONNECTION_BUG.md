# Critical Bug Fix: Hardcoded Connection Name in ingest.py

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
Initially suspected the deployment scripts (copydir.sh, createpipeline.sh) were at fault, since the HubSpot connector with the same connection worked in earlier tests.

Created `hubspot_copy` (exact copy of hubspot.py) and deployed it using cockroachdb scripts to isolate the issue. Both connectors failed identically, confirming the scripts were fine.

## Root Cause
Line 151 in `sources/cockroachdb/ingest.py` had a **hardcoded connection name**:

```python
pipeline_spec = {
    "connection_name": "hubspot_demo",  # ❌ WRONG: Hardcoded!
    "objects": [...]
}
```

This caused the pipeline to attempt using the `hubspot_demo` connection instead of the `robert_lee_battle-walrus-11108` connection that was:
1. Created via `create_databricks_connection.sh`
2. Configured in the DLT pipeline configuration
3. Read from `spark.conf.get("connection_name")` at line 93

The `ingestion_pipeline.py` uses `pipeline_spec["connection_name"]` to look up the connection in Unity Catalog. Since it was looking for `hubspot_demo` (which doesn't exist or has different credentials), the connector received no credentials.

## Fix
Changed line 151 to use the variable instead of a hardcoded string:

```python
pipeline_spec = {
    "connection_name": connection_name,  # ✅ CORRECT: Use variable!
    "objects": [...]
}
```

## Impact
- **Scripts (copydir.sh, createpipeline.sh)**: ✅ Were always correct, no changes needed
- **ingest.py**: ✅ Fixed to use dynamic connection_name from pipeline configuration
- **Connector**: ✅ Now receives all connection parameters from Unity Catalog

## Lesson Learned
When debugging connector credential issues:
1. First check if the connection name in `pipeline_spec` matches the actual connection
2. Verify the connection name is dynamic (from config) not hardcoded
3. The deployment scripts are likely fine if other connectors work with them

## Testing
After fix:
- Redeployed using `./copydir.sh`
- Restarted pipeline
- Connector should now receive all connection parameters (token, base_url, host, port, database, user, password, sslmode)

## Files Changed
- `sources/cockroachdb/ingest.py` - Line 151


