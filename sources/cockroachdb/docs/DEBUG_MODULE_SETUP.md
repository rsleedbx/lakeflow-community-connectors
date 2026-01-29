# Debug Module Setup for Databricks

## ✅ Fixed Import Issue

The notebook has been updated with proper import setup. Cell 15 now includes:

```python
# Add current directory to Python path (for Databricks)
import sys
import os

# Get the directory where this notebook is located
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
notebook_dir = os.path.dirname(notebook_path)

# Add to sys.path if not already there
if notebook_dir not in sys.path:
    sys.path.insert(0, notebook_dir)
    print(f"✅ Added to sys.path: {notebook_dir}")

# Now import debug utilities
from cockroachdb_debug import (
    diagnose_column_family_sync,
    analyze_cdc_events_by_column_family,
    check_merge_completeness,
    inspect_raw_cdc_files,
    run_full_diagnosis
)
print("✅ Successfully imported cockroachdb_debug module")
```

## 📦 Setup Options

### Option 1: Upload Module to Same Directory as Notebook (Recommended)

1. **Upload `cockroachdb_debug.py` to Databricks:**
   ```bash
   databricks workspace import \
     cockroachdb_debug.py \
     /Users/your_email@company.com/cockroachdb_debug.py \
     --language PYTHON \
     --format SOURCE
   ```

2. **Ensure both files are in the same directory:**
   ```
   /Users/your_email@company.com/
   ├── cockroachdb-cdc-tutorial.ipynb
   └── cockroachdb_debug.py
   ```

3. **Run Cell 15** - It will automatically add the directory to `sys.path`

### Option 2: Use DBFS (Databricks File System)

1. **Upload to DBFS:**
   ```bash
   databricks fs cp cockroachdb_debug.py dbfs:/FileStore/cockroachdb_debug.py
   ```

2. **In Cell 15, replace the import section with:**
   ```python
   import sys
   sys.path.insert(0, '/dbfs/FileStore')
   
   from cockroachdb_debug import run_full_diagnosis
   ```

### Option 3: Install as Notebook-Scoped Library

1. **Create a wheel package:**
   ```bash
   # Create setup.py
   cat > setup.py << 'EOF'
   from setuptools import setup
   
   setup(
       name="cockroachdb_debug",
       version="1.0.0",
       py_modules=["cockroachdb_debug"],
   )
   EOF
   
   # Build wheel
   python setup.py bdist_wheel
   ```

2. **Upload wheel to Databricks:**
   - Go to Workspace → Settings → Libraries
   - Upload the `.whl` file from `dist/` directory
   - Attach to your cluster

3. **In Cell 15, simply:**
   ```python
   from cockroachdb_debug import run_full_diagnosis
   ```

## 🔧 Troubleshooting

### Error: "ModuleNotFoundError: No module named 'cockroachdb_debug'"

**Solution 1:** Check file location
```python
# Add this to Cell 15 to debug
import os
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
notebook_dir = os.path.dirname(notebook_path)
print(f"Notebook directory: {notebook_dir}")

# List files in directory
files = dbutils.fs.ls(f"file:{notebook_dir}")
print(f"Files in directory: {[f.name for f in files]}")
```

**Solution 2:** Manually specify path
```python
import sys
sys.path.insert(0, '/Workspace/Users/your_email@company.com')
```

### Error: "ImportError: cannot import name 'run_full_diagnosis'"

**Solution:** Ensure you uploaded the latest version of `cockroachdb_debug.py`

Re-upload the file:
```bash
databricks workspace import \
  cockroachdb_debug.py \
  /Users/your_email@company.com/cockroachdb_debug.py \
  --language PYTHON \
  --format SOURCE \
  --overwrite
```

### Error: "AttributeError: module 'cockroachdb_debug' has no attribute 'run_full_diagnosis'"

**Solution:** Restart Python kernel (detach and reattach notebook to cluster)

In Databricks UI:
1. Click on cluster name (top right)
2. Click "Detach"
3. Click "Attach"
4. Re-run Cell 15

## ✅ Verification

Run this in a cell to verify setup:

```python
import sys
print(f"Python path: {sys.path[:3]}")

try:
    from cockroachdb_debug import run_full_diagnosis
    print("✅ Module imported successfully!")
    
    # Check available functions
    import cockroachdb_debug
    functions = [f for f in dir(cockroachdb_debug) if not f.startswith('_')]
    print(f"✅ Available functions ({len(functions)}): {', '.join(functions[:5])}...")
    
except ImportError as e:
    print(f"❌ Import failed: {e}")
```

Expected output:
```
Python path: ['/Workspace/Users/your_email@company.com', ...]
✅ Module imported successfully!
✅ Available functions (14): analyze_cdc_events_by_column_family, check_merge_completeness, compare_row_by_row, diagnose_column_family_sync, get_column_families...
```

## 🚀 Quick Test

After successful import, test with a simple function:

```python
from cockroachdb_debug import get_column_families

conn = get_cockroachdb_connection()
try:
    families = get_column_families(conn, source_table)
    print(f"✅ Found {len(families)} column families")
    for family_name, columns in families.items():
        print(f"   📁 {family_name}: {len(columns)} columns")
finally:
    conn.close()
```

## 📖 Next Steps

Once import works:
1. Run Cell 15 to execute full diagnosis
2. Review output to identify root cause
3. Fix MERGE logic in Cell 6 based on findings
4. Re-run Cell 12 to re-ingest
5. Verify with Cell 14

## 🆘 Still Having Issues?

If none of the above work, you can also:

1. **Inline the code:** Copy the entire `cockroachdb_debug.py` content into a cell before Cell 15
2. **Use `%run`:** If you have the file in DBFS:
   ```python
   %run /path/to/cockroachdb_debug.py
   ```
3. **Contact support:** Provide the output from the verification cell above
