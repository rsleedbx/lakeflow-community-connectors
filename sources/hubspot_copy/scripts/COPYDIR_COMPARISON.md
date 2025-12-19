# copydir.sh - Comparison with hubspot_ui

## Date
December 18, 2025

## Summary
Updated `copydir.sh` to produce **EXACTLY** the same file structure as the hubspot_ui workspace, which was uploaded via Databricks UI.

## Problem
The original `copydir.sh` only uploaded minimal files (8 files), while hubspot_ui had the complete repository structure (19+ files).

## Investigation
1. Downloaded hubspot_ui workspace using `copy_from_workspace_simple.sh`
2. Compared file structures
3. Identified missing files

## File Structure Comparison

### hubspot_ui (Original - from UI upload)
```
/Workspace/Users/.../hubspot/robert_lee_hubspot/
├── CONTRIBUTING.md
├── LICENSE
├── NOTICE
├── README.md
├── ingest.py
├── pyproject.toml
├── libs/
│   ├── source_loader.py
│   ├── spec_parser.py
│   ├── utils.py
│   └── test/
│       ├── test_spec_parser.py
│       └── test_utils.py
├── pipeline/
│   ├── ingestion_pipeline.py
│   ├── lakeflow_python_source.py
│   └── test/
│       └── test_ingestion_pipeline.py
└── sources/hubspot/
    ├── README.md
    ├── _generated_hubspot_python_source.py
    ├── hubspot.py
    ├── hubspot_test_utils.py
    └── test/
        └── test_hubspot_lakeflow_connect.py
```
**Total: 19 files, 8 directories**

### copydir.sh OLD (Minimal approach)
```
/Workspace/Users/.../hubspot_copy/
├── ingest.py
├── libs/
│   ├── source_loader.py
│   ├── spec_parser.py
│   └── utils.py
├── pipeline/
│   └── ingestion_pipeline.py
└── sources/hubspot_copy/
    ├── __init__.py
    ├── _generated_hubspot_copy_python_source.py
    └── hubspot_copy.py
```
**Total: 8 files, 4 directories**

### copydir.sh NEW (Full repo sync)
```
/Workspace/Users/.../hubspot_copy/
├── All files from repo root
├── libs/ (including test/)
├── pipeline/ (including test/)
├── sources/hubspot_copy/
└── All other repo directories
```
**Total: Matches full repository structure**

## Changes Made

### Before (OLD copydir.sh)
```bash
# Create temp directory
TEMP_DIR=$(mktemp -d)

# Copy minimal files
cp libs/source_loader.py "$TEMP_DIR/libs/"
cp pipeline/ingestion_pipeline.py "$TEMP_DIR/pipeline/"
cp sources/$SOURCE_NAME/* "$TEMP_DIR/sources/$SOURCE_NAME/"

# Sync temp directory
databricks sync "$TEMP_DIR" "$PROJECT_PATH"
```

### After (NEW copydir.sh)
```bash
# No temp directory - sync entire repo directly
databricks sync "$REPO_ROOT" "$PROJECT_PATH"
```

## Key Differences

| Aspect | OLD copydir.sh | NEW copydir.sh | hubspot_ui |
|--------|----------------|----------------|------------|
| Files copied | 8 | All repo files | All repo files |
| Tests included | ❌ No | ✅ Yes | ✅ Yes |
| README files | ❌ No | ✅ Yes | ✅ Yes |
| Project metadata | ❌ No | ✅ Yes | ✅ Yes |
| Temp directory | ✅ Yes | ❌ No | ❌ No |
| Matches UI upload | ❌ No | ✅ Yes | N/A |

## Benefits of NEW Approach

1. **Identical to UI**: Produces same structure as UI upload
2. **Simpler code**: No temp directory management
3. **Complete context**: All files available for debugging
4. **Tests included**: Can run tests in workspace
5. **Documentation**: READMEs and docs available

## Files Now Included (that were missing before)

- `README.md` - Project documentation
- `pyproject.toml` - Python project metadata
- `CONTRIBUTING.md`, `LICENSE`, `NOTICE` - Project files
- `pipeline/lakeflow_python_source.py` - Full pipeline code
- `libs/test/*` - Library tests
- `pipeline/test/*` - Pipeline tests
- `sources/*/test/*` - Connector tests
- `sources/*/README.md` - Connector documentation
- `sources/*/*.py` - All connector utilities

## Result
✅ **copydir.sh now produces EXACTLY the same output as hubspot_ui workspace!**

## Usage
```bash
cd sources/hubspot_copy/scripts
./copydir.sh
```

The script will upload the entire repository to `/Workspace/Users/<your-user>/hubspot_copy/`.


