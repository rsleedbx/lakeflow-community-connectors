# Checkpoint Cleanup Performance Issue

## Problem

Clearing the checkpoint takes **over 8 minutes**, which is abnormal.

```python
# This should take seconds, not 8 minutes!
dbutils.fs.rm(f"{CHECKPOINT_PATH}/delta", True)
spark.sql(f"DROP TABLE IF EXISTS {TARGET_TABLE_PATH}")
```

## Root Causes

### 1. Accumulated Checkpoint Files

**Unity Catalog volumes** can accumulate thousands of checkpoint files from:
- Multiple test runs
- Streaming query restarts
- Failed writes
- Schema evolution

**Check checkpoint size:**
```python
# See how many files are in checkpoint
files = dbutils.fs.ls(f"{CHECKPOINT_PATH}/delta")
print(f"Checkpoint files: {len(files)}")

# Check total size
total_size = sum(f.size for f in files)
print(f"Total size: {total_size / (1024**2):.2f} MB")
```

### 2. Delta Table History

**Delta tables accumulate:**
- Transaction log files (one per write)
- Data files from each version
- Metadata files

**Check table history:**
```python
# See table history
display(spark.sql(f"DESCRIBE HISTORY {TARGET_TABLE_PATH}"))

# Count versions
history_df = spark.sql(f"DESCRIBE HISTORY {TARGET_TABLE_PATH}")
version_count = history_df.count()
print(f"Table versions: {version_count}")
```

### 3. Unity Catalog Volume Overhead

Unity Catalog volumes may have:
- Additional metadata tracking
- Access control checks
- Slower file operations vs DBFS

## Solutions

### Solution 1: Use VACUUM to Clean Up (Recommended)

```python
print("="*80)
print("OPTIMIZED CHECKPOINT CLEARING")
print("="*80)

import time
start = time.time()

try:
    # 1. Drop table first (faster)
    print("1️⃣ Dropping table...")
    spark.sql(f"DROP TABLE IF EXISTS {TARGET_TABLE_PATH}")
    print(f"   ✅ Done in {time.time() - start:.1f}s")
    
    # 2. Clear checkpoint (should be faster now)
    print("2️⃣ Clearing checkpoint...")
    checkpoint_start = time.time()
    dbutils.fs.rm(f"{CHECKPOINT_PATH}/delta", True)
    print(f"   ✅ Done in {time.time() - checkpoint_start:.1f}s")
    
    # 3. Clear schema location too
    print("3️⃣ Clearing schema...")
    dbutils.fs.rm(f"{CHECKPOINT_PATH}/schema", True)
    print(f"   ✅ Done in {time.time() - checkpoint_start:.1f}s")
    
    print(f"\n✅ Total time: {time.time() - start:.1f}s")
    
except Exception as e:
    print(f"⚠️  Error: {e}")
    print(f"   Time elapsed: {time.time() - start:.1f}s")

print("="*80)
```

### Solution 2: Clear Entire Checkpoint Directory

```python
# Clear ALL checkpoints for this table (more aggressive)
try:
    # Remove entire checkpoint tree
    dbutils.fs.rm(f"{CHECKPOINT_PATH}", True)
    print("✅ Cleared entire checkpoint directory")
except Exception as e:
    print(f"ℹ️  Checkpoint may not exist: {e}")

# Drop table
spark.sql(f"DROP TABLE IF EXISTS {TARGET_TABLE_PATH}")
print("✅ Dropped table")
```

### Solution 3: Use Different Checkpoint Per Test

```python
# Instead of reusing same checkpoint, create unique ones
import datetime
timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

CHECKPOINT_PATH = f"{VOLUME_PATH}/_checkpoints/{SOURCE_TABLE}_{timestamp}"

# No need to clear - each run gets fresh checkpoint
# Old checkpoints can be cleaned up periodically
```

### Solution 4: Skip Checkpoint Clearing for Incremental Tests

```python
# Only clear checkpoint when needed
CLEAR_CHECKPOINT = False  # ⭐ Set to True only for fresh start

if CLEAR_CHECKPOINT:
    print("🧹 Clearing checkpoint...")
    dbutils.fs.rm(f"{CHECKPOINT_PATH}/delta", True)
    spark.sql(f"DROP TABLE IF EXISTS {TARGET_TABLE_PATH}")
    print("✅ Cleared!")
else:
    print("ℹ️  Skipping checkpoint clear (incremental mode)")
```

## Investigation Script

Run this to diagnose the issue:

```python
print("="*80)
print("CHECKPOINT DIAGNOSTICS")
print("="*80)

import time

# Configuration
CHECKPOINT_PATH = f"{VOLUME_PATH}/_checkpoints/{SOURCE_TABLE}"
TARGET_TABLE_PATH = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{TARGET_TABLE}"

print(f"Checkpoint path: {CHECKPOINT_PATH}")
print(f"Target table: {TARGET_TABLE_PATH}")
print()

# 1. Check checkpoint size
print("1️⃣ Checking checkpoint directory...")
try:
    delta_path = f"{CHECKPOINT_PATH}/delta"
    files = dbutils.fs.ls(delta_path)
    file_count = len(files)
    total_size = sum(f.size for f in files)
    
    print(f"   Files: {file_count:,}")
    print(f"   Size: {total_size / (1024**2):.2f} MB")
    
    if file_count > 1000:
        print(f"   ⚠️  WARNING: {file_count:,} files is excessive!")
    elif file_count > 100:
        print(f"   ⚠️  Note: {file_count} files may slow down cleanup")
    else:
        print(f"   ✅ File count is normal")
except Exception as e:
    print(f"   ℹ️  No checkpoint exists: {e}")

print()

# 2. Check table history
print("2️⃣ Checking Delta table history...")
try:
    history = spark.sql(f"DESCRIBE HISTORY {TARGET_TABLE_PATH}")
    version_count = history.count()
    
    print(f"   Versions: {version_count:,}")
    
    if version_count > 100:
        print(f"   ⚠️  WARNING: {version_count:,} versions may slow down DROP")
    elif version_count > 20:
        print(f"   ⚠️  Note: {version_count} versions detected")
    else:
        print(f"   ✅ Version count is normal")
    
    # Show latest version
    latest = history.orderBy("version", ascending=False).first()
    print(f"   Latest version: {latest['version']} at {latest['timestamp']}")
    
except Exception as e:
    print(f"   ℹ️  Table doesn't exist: {e}")

print()

# 3. Test cleanup speed
print("3️⃣ Testing cleanup speed...")
start = time.time()

try:
    # Drop table
    table_start = time.time()
    spark.sql(f"DROP TABLE IF EXISTS {TARGET_TABLE_PATH}")
    table_time = time.time() - table_start
    print(f"   DROP TABLE: {table_time:.1f}s")
    
    # Clear checkpoint
    checkpoint_start = time.time()
    dbutils.fs.rm(f"{CHECKPOINT_PATH}/delta", True)
    checkpoint_time = time.time() - checkpoint_start
    print(f"   Clear checkpoint: {checkpoint_time:.1f}s")
    
    total_time = time.time() - start
    print(f"   TOTAL: {total_time:.1f}s")
    
    if total_time > 60:
        print(f"   ❌ SLOW: {total_time:.1f}s is too long!")
    elif total_time > 10:
        print(f"   ⚠️  SLOW: {total_time:.1f}s is slower than expected")
    else:
        print(f"   ✅ FAST: {total_time:.1f}s is normal")
        
except Exception as e:
    print(f"   Error: {e}")

print("="*80)
```

## Expected vs Actual Times

| Operation | Expected | Your Experience | Issue |
|-----------|----------|-----------------|-------|
| DROP TABLE | 1-5s | ? | May have many versions |
| Clear checkpoint | 1-10s | 8+ min | **PROBLEM HERE** |
| **TOTAL** | **2-15s** | **8+ min** | ❌ Too slow |

## Recommendations

### Immediate Fix

```python
# Use optimized clearing (Solution 1 above)
# This should reduce time from 8 min → 10-30 seconds
```

### Long-term Fix

```python
# Option A: Use timestamped checkpoints (Solution 3)
# - No clearing needed
# - Clean up periodically

# Option B: VACUUM regularly
spark.sql(f"VACUUM {TARGET_TABLE_PATH} RETAIN 0 HOURS")
dbutils.fs.rm(f"{CHECKPOINT_PATH}", True)
```

### Prevention

1. **Don't run the same test 100s of times** without cleanup
2. **Use timestamped checkpoints** for different test runs
3. **Periodically clean up old checkpoints**
4. **Set retention period** on Delta tables

## Quick Test

Run this to see if the issue persists:

```python
import time

# Time the clearing operation
start = time.time()

# Just the checkpoint clear (not the table drop)
dbutils.fs.rm(f"{CHECKPOINT_PATH}/delta", True)

elapsed = time.time() - start
print(f"Checkpoint clear took: {elapsed:.1f}s")

if elapsed > 30:
    print("❌ PROBLEM: This is too slow!")
    print("   Check checkpoint size with diagnostics script above")
else:
    print("✅ OK: Time is acceptable")
```

## Root Cause Summary

**Most likely cause**: Checkpoint directory has accumulated **thousands of files** from repeated test runs, and Unity Catalog volume operations are slower than regular DBFS.

**Fix**: Use timestamped checkpoints or clear less frequently.


