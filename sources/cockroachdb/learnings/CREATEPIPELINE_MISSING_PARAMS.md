# createpipeline.sh - Missing Parameters Analysis

## Date
December 18, 2025

## Comparison

### Target Pipeline Definition (hubspot)
```yaml
resources:
  pipelines:
    pipeline_robert_lee_hubspot:
      name: robert_lee_hubspot                      # ✅ Present
      libraries:                                     # ⚠️  Different format
        - glob:
            include: /Workspace/.../ingest.py
      schema: robert_lee_hubspot                    # ✅ Present
      development: true                              # ✅ Present
      channel: PREVIEW                               # ❌ MISSING
      catalog: main                                  # ✅ Present
      serverless: true                               # ✅ Present
      root_path: /Workspace/.../robert_lee_hubspot   # ❌ MISSING
```

### Current createpipeline.sh (lines 108-128)
```json
{
  "name": "$PIPELINE_NAME",           // ✅ Present
  "catalog": "main",                  // ✅ Present
  "schema": "$PIPELINE_NAME",         // ✅ Present
  "configuration": {                  // ✅ Present (extra, not in hubspot)
    "source_name": "...",
    "connection_name": "...",
    "table_list": "..."
  },
  "serverless": true,                 // ✅ Present
  "continuous": false,                // ✅ Present (extra, not in hubspot)
  "development": true,                // ✅ Present
  "libraries": [                      // ⚠️  Wrong format
    {
      "file": {
        "path": "$PROJECT_PATH/ingest.py"
      }
    }
  ]
}
```

## Missing Parameters

### 1. ❌ `channel: "PREVIEW"`
**Status**: Not present in createpipeline.sh

**What it does**: Specifies the DLT channel/version to use
- `CURRENT` - Stable release
- `PREVIEW` - Preview features

**Should add**:
```json
"channel": "PREVIEW"
```

### 2. ❌ `root_path: "/Workspace/Users/..."`
**Status**: Not present in createpipeline.sh

**What it does**: Sets the root directory for pipeline files and artifacts

**Should add**:
```json
"root_path": "/Workspace/Users/$USER_NAME/$PROJECT_NAME"
```

### 3. ⚠️  `libraries` Format Difference
**Status**: Uses `file` instead of `glob`

**Current**:
```json
"libraries": [
  {
    "file": {
      "path": "/Workspace/.../ingest.py"
    }
  }
]
```

**Target (hubspot)**:
```json
"libraries": [
  {
    "glob": {
      "include": "/Workspace/.../ingest.py"
    }
  }
]
```

**Difference**:
- `file` - Specifies a single file
- `glob` - Can use wildcards/patterns to include multiple files

**Impact**: Functionally similar for single file, but hubspot uses `glob` format

## Extra Parameters in createpipeline.sh

These parameters are in createpipeline.sh but NOT in hubspot pipeline:

### 1. `configuration`
```json
"configuration": {
  "source_name": "cockroachdb",
  "connection_name": "...",
  "table_list": "..."
}
```
**Status**: ✅ This is GOOD - needed to pass parameters to ingest.py

### 2. `continuous: false`
```json
"continuous": false
```
**Status**: ✅ This is fine - controls whether pipeline runs continuously or on-demand

## Summary Table

| Parameter | createpipeline.sh | hubspot | Status |
|-----------|-------------------|---------|--------|
| `name` | ✅ Present | ✅ Present | ✅ Match |
| `catalog` | ✅ Present | ✅ Present | ✅ Match |
| `schema` | ✅ Present | ✅ Present | ✅ Match |
| `serverless` | ✅ Present | ✅ Present | ✅ Match |
| `development` | ✅ Present | ✅ Present | ✅ Match |
| `channel` | ❌ **Missing** | ✅ Present | ❌ **ADD THIS** |
| `root_path` | ❌ **Missing** | ✅ Present | ❌ **ADD THIS** |
| `libraries` | ⚠️  `file` format | ⚠️  `glob` format | ⚠️  **CHANGE FORMAT** |
| `configuration` | ✅ Present | ❌ Not in hubspot | ℹ️  Extra (OK) |
| `continuous` | ✅ Present | ❌ Not in hubspot | ℹ️  Extra (OK) |

## Action Items

To match the hubspot pipeline definition, `createpipeline.sh` should add:

1. **Add `channel: "PREVIEW"`** (line ~117)
2. **Add `root_path: "$PROJECT_PATH"`** (line ~117)
3. **Consider changing `libraries.file` to `libraries.glob`** (lines 121-127)
   - Note: Both formats work, but hubspot uses `glob`

## Recommendation

**Priority 1 (Required)**:
- ✅ Add `channel: "PREVIEW"`
- ✅ Add `root_path`

**Priority 2 (Optional)**:
- Change `libraries.file.path` to `libraries.glob.include` (cosmetic, both work)

The `configuration` and `continuous` parameters are fine to keep as they provide useful functionality not present in the hubspot example.





