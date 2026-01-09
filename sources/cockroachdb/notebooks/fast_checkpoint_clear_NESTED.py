"""
FINAL VERSION - Fast Checkpoint Clear with Nested Directory Support

Handles checkpoint structures like:
  checkpoint/
    delta/
      state/0/  <- hundreds of small files
      state/1/
      state/2/
    schema/
      ...

Goes 2 levels deep to parallelize deletion of nested directories.

Copy this entire cell and run it in your notebook.
"""

# ============================================================================
# FAST CHECKPOINT CLEARING (Nested Directory Support)
# ============================================================================

import time
from concurrent.futures import ThreadPoolExecutor

print("="*80)
print("FAST CHECKPOINT CLEARING (NESTED)")
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
    
    print(f"📊 Found {len(dirs)} top-level directories, {len(files)} files\n")
    
    if dirs:
        # Go 2 levels deep to find nested checkpoint directories
        print(f"🔍 Scanning for nested directories...")
        
        all_nested_dirs = []
        for top_dir in dirs:
            try:
                nested_items = dbutils.fs.ls(top_dir)
                nested_found = 0
                for item in nested_items:
                    is_dir = False
                    try:
                        is_dir = item.isDir()
                    except (AttributeError, TypeError):
                        try:
                            is_dir = item.isDir
                        except AttributeError:
                            is_dir = item.path.endswith('/') or '.' not in item.name
                    
                    if is_dir:
                        all_nested_dirs.append(item.path)
                        nested_found += 1
                
                top_name = top_dir.rstrip('/').split('/')[-1]
                print(f"   {top_name}/: {nested_found} subdirectories")
            except Exception as e:
                # If can't list, just add the top dir for deletion
                all_nested_dirs.append(top_dir)
                print(f"   {top_dir.rstrip('/').split('/')[-1]}/: (will delete as-is)")
        
        print(f"\n   Total nested directories: {len(all_nested_dirs)}")
        print(f"\n🔥 Deleting {len(all_nested_dirs)} directories in parallel...")
        
        def delete_subdir(path):
            try:
                dbutils.fs.rm(path, True)
                # Show parent/child for better context
                name = path.rstrip('/').split('/')[-1]
                parts = path.rstrip('/').split('/')
                parent = parts[-2] if len(parts) > 1 else ''
                display_name = f"{parent}/{name}" if parent else name
                print(f"   ✅ {display_name}")
                return True
            except Exception as e:
                name = path.rstrip('/').split('/')[-1]
                parts = path.rstrip('/').split('/')
                parent = parts[-2] if len(parts) > 1 else ''
                display_name = f"{parent}/{name}" if parent else name
                print(f"   ❌ {display_name}: {e}")
                return False
        
        with ThreadPoolExecutor(max_workers=20) as executor:
            results = list(executor.map(delete_subdir, all_nested_dirs))
            deleted_count = sum(results)
            failed_count = len([r for r in results if not r])
        
        print(f"\n   ✅ Deleted {deleted_count} directories")
        if failed_count > 0:
            print(f"   ⚠️  Failed: {failed_count} directories")
    
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
        print(f"   ✅ Deleted parent")
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

if deleted_count > 50:
    sequential_est = deleted_count * 0.5
    speedup = sequential_est / elapsed if elapsed > 0 else 1
    print(f"📊 Sequential estimate: {sequential_est:.1f}s")
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


