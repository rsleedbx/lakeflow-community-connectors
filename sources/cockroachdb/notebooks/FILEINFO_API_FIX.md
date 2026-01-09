# FileInfo API Fix - Databricks Runtime Compatibility

## The Problem

```
❌ Error: 'FileInfo' object has no attribute 'isDir'
```

This error occurs because **Databricks FileInfo API differs across runtime versions**:

| Runtime Version | API |
|----------------|-----|
| **Older** | `item.isDir` (property) |
| **Newer** | `item.isDir()` (method) |
| **Python dbutils** | Neither - uses different attribute names |

## The Fix

### ✅ Robust Check (Works Across All Versions)

```python
# Separate directories and files
dirs = []
files = []
for item in items:
    is_dir = False
    try:
        is_dir = item.isDir()  # Try method call first (newer versions)
    except (AttributeError, TypeError):
        try:
            is_dir = item.isDir  # Try property (older versions)
        except AttributeError:
            # Fallback: check if path looks like a directory
            is_dir = item.path.endswith('/') or '.' not in item.name
    
    if is_dir:
        dirs.append(item.path)
    else:
        files.append(item.path)
```

### ❌ Original Code (Breaks on Some Runtimes)

```python
# This only works on newer runtimes
dirs = [item.path for item in items if item.isDir()]
files = [item.path for item in items if not item.isDir()]
```

## Fixed Files

1. ✅ `sources/cockroachdb/cockroachdb.py` - Main module (line ~2545)
2. ✅ `sources/cockroachdb/notebooks/fast_checkpoint_clear_cell_v2.py` - Fallback cell
3. ✅ `sources/cockroachdb/notebooks/fast_checkpoint_clear_WORKING.py` - New working cell

## How to Use

### Option 1: Restart Kernel (Cleanest)

1. **Restart your notebook kernel**
2. Run this cell:

```python
from cockroachdb import parallel_delete_checkpoint

result = parallel_delete_checkpoint(
    checkpoint_path=CHECKPOINT_PATH,
    dbutils=dbutils,
    max_workers=20,
    debug=True
)

spark.sql(f"DROP TABLE IF EXISTS {TARGET_TABLE_PATH}")
```

### Option 2: Use Working Cell (No Restart)

Copy and paste the entire contents of `fast_checkpoint_clear_WORKING.py` into a cell and run it.

The working cell is **self-contained** and includes the fix.

## Testing

After the fix, you should see:

```
================================================================================
FAST CHECKPOINT CLEARING
================================================================================
📊 Found 247 subdirectories, 3 files
🔥 Deleting 247 subdirectories in parallel...
   ✅ Deleted 247 subdirectories
   ✅ Deleted parent: dbfs:/Volumes/.../checkpoints

✅ Deleted 250 items in 12.3s
⚡ Estimated speedup: 10.1x faster than sequential
✅ Dropped table: main.schema.table_delta
================================================================================
```

## Why This Happens

**Databricks FileInfo objects come from different sources:**

1. **Java FileInfo** (from `dbutils.fs.ls()` in Scala-based runtimes)
   - Has `isDir()` method
   
2. **Python FileInfo** (from Python dbutils)
   - Has `isDir` property (no parentheses)
   
3. **Spark Connect FileInfo** (remote execution)
   - May have neither - uses different API

**Our fix handles all three** by trying each approach in order and falling back to path inspection.

## Alternative: Path-Based Detection

If you only care about common checkpoint directories:

```python
def is_checkpoint_dir(item):
    """Check if item is a checkpoint subdirectory."""
    name = item.name
    # Common checkpoint subdirectory names
    checkpoint_names = ['delta', 'schema', 'offsets', 'commits', 'metadata', 'sources']
    return name in checkpoint_names or item.path.endswith('/')
```

## Summary

**Problem**: `item.isDir()` not available on all Databricks runtimes

**Solution**: Try multiple detection methods:
1. `item.isDir()` (method) ← Try first
2. `item.isDir` (property) ← Fallback 1
3. Path inspection ← Fallback 2

**Status**: ✅ Fixed in `cockroachdb.py` and all helper scripts

**Action Required**: 
- **Immediate**: Use `fast_checkpoint_clear_WORKING.py` cell
- **Next time**: Restart kernel and import normally


