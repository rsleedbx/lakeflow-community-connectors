# Databricks Pipelines: Asset Bundles vs REST API Format

## Date
December 18, 2025

## Problem
The hubspot pipeline definition provided used Databricks Asset Bundles (YAML) format, which has different field names than the Databricks Pipelines REST API (JSON) format.

## Two Pipeline Definition Formats

### 1. Databricks Asset Bundles (YAML)
Used for declarative infrastructure-as-code deployment via `databricks bundle` CLI.

```yaml
resources:
  pipelines:
    my_pipeline:
      name: robert_lee_hubspot
      catalog: main
      schema: robert_lee_hubspot
      serverless: true
      development: true
      channel: PREVIEW
      root_path: /Workspace/Users/.../robert_lee_hubspot  # ← Bundles only
      libraries:
        - glob:                                            # ← Bundles format
            include: /Workspace/.../ingest.py
```

### 2. Databricks Pipelines REST API (JSON)
Used for programmatic pipeline creation via `databricks pipelines create` CLI.

```json
{
  "name": "robert_lee_hubspot",
  "catalog": "main",
  "schema": "robert_lee_hubspot",
  "serverless": true,
  "development": true,
  "channel": "PREVIEW",
  "continuous": false,
  "configuration": {...},
  "libraries": [
    {
      "file": {                                           // ← REST API format
        "path": "/Workspace/.../ingest.py"
      }
    }
  ]
}
```

## Key Differences

| Field | Asset Bundles (YAML) | REST API (JSON) | Notes |
|-------|---------------------|-----------------|-------|
| `root_path` | ✅ Supported | ❌ Not supported | Only in Bundles |
| `libraries` | `glob.include` | `file.path` | Different structure |
| `configuration` | ❌ Rare | ✅ Common | For passing params to pipeline |
| `continuous` | ❌ Optional | ✅ Common | Controls continuous vs triggered |

## Error Encountered

When using Asset Bundles fields in REST API:

```bash
Warning: unknown field: root_path
  at 
  in (inline):14:4

Warning: unknown field: glob
  at libraries[0]
  in (inline):17:8
```

**Result**: Pipeline still created (API ignores unknown fields), but script failed to parse response due to warnings.

## Solution

**createpipeline.sh uses REST API format:**

```json
{
  "name": "$name",
  "catalog": "main",
  "schema": "$schema",
  "configuration": {...},
  "serverless": true,
  "continuous": false,
  "development": true,
  "channel": "PREVIEW",        // ✅ Supported in REST API
  "libraries": [
    {
      "file": {                // ✅ Correct REST API format
        "path": "$ingest_path"
      }
    }
  ]
}
```

**Removed:**
- ❌ `root_path` - Asset Bundles only
- ❌ `libraries.glob.include` - Asset Bundles format

**Kept:**
- ✅ `channel: "PREVIEW"` - Supported in REST API
- ✅ `libraries.file.path` - Correct REST API format

## Which Format to Use?

### Use Asset Bundles (YAML) when:
- Deploying entire infrastructure as code
- Need version control for all resources
- Want declarative configuration
- Using `databricks bundle deploy`

### Use REST API (JSON) when:
- Creating pipelines programmatically
- Using `databricks pipelines create` CLI
- Need dynamic pipeline creation
- **← This is what createpipeline.sh does**

## Resources

- [Databricks Asset Bundles Docs](https://docs.databricks.com/en/dev-tools/bundles/index.html)
- [Databricks Pipelines REST API](https://docs.databricks.com/api/workspace/pipelines)
- [DLT API Reference](https://docs.databricks.com/api/workspace/pipelines/create)

## Conclusion

✅ **createpipeline.sh now uses correct REST API format** with only supported fields (`channel`, `file.path`).

The hubspot pipeline YAML is valid for Asset Bundles but not directly compatible with the REST API used by `databricks pipelines create`.





