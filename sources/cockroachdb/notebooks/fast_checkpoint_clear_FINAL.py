"""
FINAL WORKING VERSION - Fast Checkpoint Clear
Copy this entire cell and run it in your notebook.

All fixes included:
- ✅ FileInfo API compatibility (isDir/isDir())  
- ✅ Directory name display (handles trailing slashes)
- ✅ Parallel deletion (20 workers)
- ✅ Progress output with directory names
"""

# ============================================================================
# FAST CHECKPOINT CLEARING (All Fixes Applied)
# ============================================================================

import time
from concurrent.futures import ThreadPoolExecutor

print("="*80)
print("FAST CHECKPOINT CLEARING")
print("="*80)

start_time = time.time()
deleted_count = 0
failed_count = 0

try:
    items = dbutils.fs.ls(CHECKPOINT_PATH)
    
    # Separate directories and files - handle different FileInfo APIs
    dirs = []
    files = []
    for item in items:
        is_dir = False
        try:
            is_dir = item.isDir()  # Method (newer)
        except (AttributeError, TypeError):
            try:
                is_dir = item.isDir  # Property (older)
            except AttributeError:
                is_dir = item.path.endswith('/') or '.' not in item.name
        
        if is_dir:
            dirs.append(item.path)
        else:
            files.append(item.path)
    
    print(f"📊 Found {len(dirs)} subdirectories, {len(files)} files\n")
    
    if dirs:
        print(f"🔥 Deleting {len(dirs)} subdirectories in parallel...")
        
        def delete_subdir(path):
            try:
                dbutils.fs.rm(path, True)
                # Strip trailing slash to get proper name
                name = path.rstrip('/').split('/')[-1]
                print(f"   ✅ {name}")
                return True
            except Exception as e:
                name = path.rstrip('/').split('/')[-1]
                print(f"   ❌ {name}: {e}")
                return False
        
        with ThreadPoolExecutor(max_workers=20) as executor:
            results = list(executor.map(delete_subdir, dirs))
            deleted_count = sum(results)
            failed_count = len([r for r in results if not r])
        
        print(f"\n   Deleted {deleted_count} subdirectories")
        if failed_count > 0:
            print(f"   Failed: {failed_count} subdirectories")
    
    # Delete remaining files
    if files:
        print(f"\n🗑️  Deleting {len(files)} files...")
        for file_path in files:
            try:
                dbutils.fs.rm(file_path, False)
                deleted_count += 1
            except:
                failed_count += 1
    
    # Delete parent directory
    print(f"\n🧹 Cleaning up parent directory...")
    try:
        dbutils.fs.rm(CHECKPOINT_PATH, True)
        print(f"   ✅ Deleted parent: {CHECKPOINT_PATH.split('/')[-1]}")
    except Exception as e:
        print(f"   ℹ️  Parent cleanup: {e}")
        
except Exception as e:
    print(f"ℹ️  Checkpoint doesn't exist: {e}")

elapsed = time.time() - start_time

print()
print("="*80)
print("DELETION SUMMARY")
print("="*80)
print(f"✅ Deleted {deleted_count} items in {elapsed:.1f}s")

if failed_count > 0:
    print(f"⚠️  Failed: {failed_count} items")

if deleted_count > 100:
    sequential_est = deleted_count * 0.5
    speedup = sequential_est / elapsed if elapsed > 0 else 1
    print(f"📊 Estimated sequential time: {sequential_est:.1f}s")
    print(f"⚡ Speedup: {speedup:.1f}x faster")

print("="*80)

# Drop table
try:
    spark.sql(f"DROP TABLE IF EXISTS {TARGET_TABLE_PATH}")
    print(f"\n✅ Dropped table: {TARGET_TABLE_PATH}")
except Exception as e:
    print(f"ℹ️  Table drop: {e}")

print("\n" + "="*80)
print("CLEAR COMPLETE")
print("="*80)


