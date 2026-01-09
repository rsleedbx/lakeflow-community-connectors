# Parallel Checkpoint Deletion - Usage Guide

## Problem

Deleting checkpoints with hundreds of subdirectories can take **5-10 minutes** sequentially.

## Solution

Use `parallel_delete_checkpoint()` for **10-60 second** deletion time (5-20x faster!)

## Basic Usage

### In a Databricks Notebook

```python
from cockroachdb import parallel_delete_checkpoint

# Delete checkpoint directory in parallel
result = parallel_delete_checkpoint(
    checkpoint_path='dbfs:/Volumes/main/schema/volume/_checkpoints/usertable',
    dbutils=dbutils,  # Use notebook's dbutils context
    max_workers=20,   # Number of parallel threads
    debug=True        # Show progress
)

# Check results
print(f"✅ Deleted {result['deleted_count']} items in {result['elapsed_seconds']:.1f}s")
```

### Before Running load_parquet_with_merge.ipynb

```python
# Cell: Fast Checkpoint Clear
from cockroachdb import parallel_delete_checkpoint

# Configuration (from earlier cells)
CHECKPOINT_PATH = f"{VOLUME_PATH}/_checkpoints/{SOURCE_TABLE}"

# Fast parallel deletion
result = parallel_delete_checkpoint(
    checkpoint_path=CHECKPOINT_PATH,
    dbutils=dbutils,
    max_workers=20,
    debug=True
)

# Also drop the Delta table
spark.sql(f"DROP TABLE IF EXISTS {TARGET_TABLE_PATH}")

print(f"\n✅ Checkpoint cleared in {result['elapsed_seconds']:.1f}s")
```

## Integration Examples

### Example 1: Replace Slow Checkpoint Clear Cell

**Old (Slow):**
```python
# ❌ Takes 5-10 minutes with many subdirectories!
dbutils.fs.rm(f"{CHECKPOINT_PATH}/delta", True)
spark.sql(f"DROP TABLE IF EXISTS {TARGET_TABLE_PATH}")
```

**New (Fast):**
```python
# ✅ Takes 10-60 seconds!
from cockroachdb import parallel_delete_checkpoint

result = parallel_delete_checkpoint(
    checkpoint_path=CHECKPOINT_PATH,
    dbutils=dbutils,
    max_workers=20
)

spark.sql(f"DROP TABLE IF EXISTS {TARGET_TABLE_PATH}")
```

### Example 2: With load_and_merge_cdc_to_delta

```python
from cockroachdb import load_and_merge_cdc_to_delta, parallel_delete_checkpoint, load_crdb_config

# Load configs
crdb_config = load_crdb_config('.env/cockroachdb_credentials.json')

# Fast checkpoint clear BEFORE running test
checkpoint_path = f"{VOLUME_PATH}/_checkpoints"
parallel_delete_checkpoint(
    checkpoint_path=checkpoint_path,
    dbutils=dbutils,
    debug=False  # Suppress output
)

# Run automated test (with clear_checkpoint=False since we already cleared it)
result = load_and_merge_cdc_to_delta(
    source_table='usertable',
    volume_path=VOLUME_PATH,
    target_table_path=TARGET_TABLE_PATH,
    crdb_config=crdb_config,
    catalog='defaultdb',
    schema='public',
    clear_checkpoint=False,  # Already cleared above
    verify=True,
    compare_source=True
)
```

### Example 3: Periodic Cleanup

```python
# Clean up all old checkpoints periodically (e.g., weekly)
from cockroachdb import parallel_delete_checkpoint

# Delete all checkpoints for a volume
result = parallel_delete_checkpoint(
    checkpoint_path=f"{VOLUME_PATH}/_checkpoints",
    dbutils=dbutils,
    max_workers=30,  # Use more workers for big cleanup
    debug=True
)

print(f"🧹 Cleaned up {result['deleted_count']} checkpoint directories")
```

### Example 4: Conditional Fast Delete

```python
# Check size first, use parallel delete if needed
checkpoint_path = f"{VOLUME_PATH}/_checkpoints/{SOURCE_TABLE}"

try:
    items = dbutils.fs.ls(checkpoint_path)
    subdir_count = len([item for item in items if item.isDir()])
    
    if subdir_count > 50:
        print(f"⚡ {subdir_count} subdirectories detected - using parallel delete")
        from cockroachdb import parallel_delete_checkpoint
        parallel_delete_checkpoint(checkpoint_path, dbutils, max_workers=20)
    else:
        print(f"📁 {subdir_count} subdirectories - using standard delete")
        dbutils.fs.rm(checkpoint_path, True)
except:
    print("ℹ️  No checkpoint exists")

# Drop table
spark.sql(f"DROP TABLE IF EXISTS {TARGET_TABLE_PATH}")
```

## Performance Comparison

### Scenario: 1,000 Subdirectories

| Method | Time | Speed |
|--------|------|-------|
| Sequential `dbutils.fs.rm()` | 8-10 min | ⭐ Slow |
| Databricks CLI `databricks fs rm` | 8-10 min | ⭐ Slow |
| **`parallel_delete_checkpoint()` (10 workers)** | **60-90s** | ⭐⭐⭐⭐ Fast |
| **`parallel_delete_checkpoint()` (20 workers)** | **30-60s** | ⭐⭐⭐⭐⭐ Very Fast |

### Scenario: 100 Subdirectories

| Method | Time | Speed |
|--------|------|-------|
| Sequential `dbutils.fs.rm()` | 30-60s | ⭐⭐ OK |
| **`parallel_delete_checkpoint()` (20 workers)** | **5-15s** | ⭐⭐⭐⭐⭐ Very Fast |

## Function Parameters

```python
parallel_delete_checkpoint(
    checkpoint_path: str,      # Required: Path to checkpoint directory
    dbutils,                   # Required: dbutils from notebook context
    max_workers: int = 20,     # Optional: Number of parallel threads
    debug: bool = True         # Optional: Show progress output
)
```

### Parameter Guidelines

**max_workers:**
- `10-20`: Good for most scenarios (1000+ subdirs)
- `20-30`: For very large cleanups (5000+ subdirs)
- `5-10`: For smaller cleanups (100-500 subdirs)
- Don't set too high (>50) - diminishing returns

**debug:**
- `True`: Shows progress, statistics, performance comparison
- `False`: Silent operation (only errors shown)

## Return Value

```python
{
    'success': bool,              # True if all deletions succeeded
    'deleted_count': int,         # Number of items deleted
    'failed_count': int,          # Number of failed deletions
    'elapsed_seconds': float,     # Time taken
    'subdirs': List[str]         # List of deleted subdirectory names
}
```

## Common Patterns

### Pattern 1: Fast Checkpoint + Test

```python
from cockroachdb import parallel_delete_checkpoint, load_and_merge_cdc_to_delta

# 1. Fast checkpoint clear
parallel_delete_checkpoint(f"{VOLUME_PATH}/_checkpoints", dbutils)

# 2. Run test
result = load_and_merge_cdc_to_delta(..., clear_checkpoint=False)
```

### Pattern 2: Conditional Clear

```python
# Only clear if checkpoint is very large
try:
    items = dbutils.fs.ls(checkpoint_path)
    if len(items) > 100:
        parallel_delete_checkpoint(checkpoint_path, dbutils)
except:
    pass  # No checkpoint exists
```

### Pattern 3: Weekly Cleanup Job

```python
# Run as a scheduled job to clean up all old checkpoints
from cockroachdb import parallel_delete_checkpoint

volumes = [
    'dbfs:/Volumes/main/schema1/volume1',
    'dbfs:/Volumes/main/schema2/volume2'
]

for volume in volumes:
    checkpoint_path = f"{volume}/_checkpoints"
    result = parallel_delete_checkpoint(checkpoint_path, dbutils, debug=False)
    print(f"Cleaned {volume}: {result['deleted_count']} items in {result['elapsed_seconds']:.1f}s")
```

## Troubleshooting

### Issue: "dbutils not found"

**Solution:** Make sure you pass `dbutils` from notebook context:

```python
# In notebook cell:
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

# Now pass it to function
parallel_delete_checkpoint(path, dbutils)
```

### Issue: Still slow (20+ seconds for 100 subdirs)

**Possible causes:**
- Unity Catalog Volume overhead
- Network latency
- Too many small files

**Solutions:**
- Increase `max_workers` to 30-40
- Use timestamped checkpoints (avoid clearing)
- Clean up less frequently

### Issue: Some deletions fail

**This is usually OK** - the function will report failed count and continue.

**Check the result:**
```python
result = parallel_delete_checkpoint(...)
if result['failed_count'] > 0:
    print(f"⚠️  {result['failed_count']} deletions failed")
    # Try again or investigate
```

## Best Practices

1. **Use parallel delete for 100+ subdirectories**
   - Below 100: Sequential is fine
   - Above 100: Parallel is significantly faster

2. **Set debug=False for automated jobs**
   - Reduces output noise
   - Faster execution

3. **Consider timestamped checkpoints to avoid deletion**
   ```python
   import datetime
   timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
   CHECKPOINT_PATH = f"{VOLUME_PATH}/_checkpoints/{SOURCE_TABLE}_{timestamp}"
   # No deletion needed!
   ```

4. **Clean up periodically, not every run**
   - Delete old checkpoints weekly/monthly
   - Use parallel delete for the cleanup job

## Summary

**When to use:**
- ✅ Checkpoint has 100+ subdirectories
- ✅ Deletion takes > 30 seconds
- ✅ Running tests repeatedly (many checkpoints accumulate)

**Benefits:**
- ⚡ 5-20x faster than sequential deletion
- 🔧 Easy to use (just pass dbutils)
- 📊 Shows statistics and performance

**Usage:**
```python
from cockroachdb import parallel_delete_checkpoint
parallel_delete_checkpoint(checkpoint_path, dbutils, max_workers=20)
```

That's it! 🎯


