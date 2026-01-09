# Fix: Environment Variables Not Loading from .env File

## Problem

Environment variables from `cockroachdb_cdc_azure.env` were not loading in the notebook.

## Root Cause

The `.env` file is located at:
```
sources/cockroachdb_s3/scripts/cockroachdb_cdc_azure.env
```

But the notebook was only checking:
1. ❌ `{git_root}/cockroachdb_cdc_azure.env`
2. ❌ `{git_root}/sources/cockroachdb_s3/cockroachdb_cdc_azure.env`

**Missing:** The `scripts/` subdirectory!

## Solution

Updated the notebook to check all three locations:

```python
env_paths = [
    os.path.join(git_root, "cockroachdb_cdc_azure.env"),  
    os.path.join(git_root, "sources/cockroachdb_s3/cockroachdb_cdc_azure.env"),
    os.path.join(git_root, "sources/cockroachdb_s3/scripts/cockroachdb_cdc_azure.env"),  # ✅ Added!
]
```

Plus added verification to confirm variables were actually loaded:

```python
# Verify variables were loaded
loaded_vars = []
for var in ['AZURE_STORAGE_ACCOUNT', 'AZURE_STORAGE_KEY', 'COCKROACHDB_URL']:
    if os.environ.get(var):
        loaded_vars.append(var)

if loaded_vars:
    print(f"   Environment variables loaded: {', '.join(loaded_vars)}")
```

## How to Verify It Works

Run the notebook again. You should see:

```
📂 Git root: /Users/robert.lee/github/lakeflow-community-connectors
📂 Sources directory: .../sources
📂 CockroachDB directory: .../sources/cockroachdb
✅ Loaded .env from: .../sources/cockroachdb_s3/scripts/cockroachdb_cdc_azure.env
   Environment variables loaded: AZURE_STORAGE_ACCOUNT, AZURE_STORAGE_KEY, COCKROACHDB_URL
✅ Imported LakeflowConnect from cockroachdb package
```

Then in the next cell (environment check):

```
Environment Variables Check:
  AZURE_STORAGE_ACCOUNT: cockroachcdc1766161393
  COCKROACHDB_URL: ✅ Found
```

## Alternative: Copy to Repo Root (Recommended)

For easier access, you can copy the `.env` file to the repo root:

```bash
cd /Users/robert.lee/github/lakeflow-community-connectors
cp sources/cockroachdb_s3/scripts/cockroachdb_cdc_azure.env ./
```

This makes it available at the first location checked, and also works for VS Code auto-loading.

## File Locations

### Current Location ✅
```
/Users/robert.lee/github/lakeflow-community-connectors/
└── sources/
    └── cockroachdb_s3/
        └── scripts/
            └── cockroachdb_cdc_azure.env  ← Actual file
```

### Recommended Location (Optional)
```
/Users/robert.lee/github/lakeflow-community-connectors/
└── cockroachdb_cdc_azure.env  ← Copy here for easier access
```

### Backup File
```
sources/cockroachdb_s3/cockroachdb_cdc_azure.env.bak  ← Backup copy
```

## Troubleshooting

### Still not loading?

**Check 1: Is python-dotenv installed?**
```bash
pip install python-dotenv
```

**Check 2: Does the file exist?**
```bash
ls -la sources/cockroachdb_s3/scripts/cockroachdb_cdc_azure.env
```

**Check 3: Check file permissions**
```bash
cat sources/cockroachdb_s3/scripts/cockroachdb_cdc_azure.env
```

**Check 4: File format**
The file should have lines like:
```bash
export AZURE_STORAGE_ACCOUNT=cockroachcdc1766161393
export COCKROACHDB_URL="postgresql://..."
```

`python-dotenv` handles `export` statements correctly.

### Variables still empty after loading?

The notebook now prints which variables were successfully loaded. If you see:

```
✅ Loaded .env from: ...
⚠️  File loaded but no variables found (check format)
```

This means the file was read but variables weren't parsed. Check:
- File format (should be `KEY=value` or `export KEY=value`)
- No syntax errors in the file
- Values are on the same line as keys

### Manual verification

After running Cell 2, manually check:

```python
import os
print(f"AZURE_STORAGE_ACCOUNT: {os.environ.get('AZURE_STORAGE_ACCOUNT')}")
print(f"COCKROACHDB_URL: {os.environ.get('COCKROACHDB_URL')}")
```

If these print `None`, the variables weren't loaded.

## Related Issues

### python-dotenv with `export` statements

`python-dotenv` correctly handles bash-style `.env` files with `export`:

```bash
# Both formats work:
export KEY=value      ✅
KEY=value            ✅

# With quotes:
export KEY="value"   ✅
KEY="value"          ✅
```

### VS Code Auto-Loading

If you want VS Code to auto-load the `.env` file for all Python execution:

1. Copy to repo root:
   ```bash
   cp sources/cockroachdb_s3/scripts/cockroachdb_cdc_azure.env ./
   ```

2. Create/update `.vscode/settings.json`:
   ```json
   {
     "python.envFile": "${workspaceFolder}/cockroachdb_cdc_azure.env"
   }
   ```

3. Reload VS Code

## Summary

✅ **Fixed:** Notebook now checks `scripts/` subdirectory  
✅ **Enhanced:** Verifies variables were actually loaded  
✅ **Improved:** Shows which variables were found  
✅ **Robust:** Checks multiple locations with helpful error messages  

The `.env` file should now load correctly! 🎉







