"""
RECURSIVE VERSION - Fast Checkpoint Clear (3 Levels Deep)

Handles checkpoint structures like:
  checkpoint/
    delta/              ← Level 1
      state/            ← Level 2
        0/              ← Level 3 (WHERE THE FILES ARE!)
        1/
        2/
        ...
      commits/
    schema/
      _schemas/

Recursively scans up to 3 levels deep to find ALL leaf directories.

Copy this entire cell and run it in your notebook.
"""

# ============================================================================
# FAST CHECKPOINT CLEARING (Recursive, 3 Levels)
# ============================================================================

import time
from concurrent.futures import ThreadPoolExecutor

print("="*80)
print("FAST CHECKPOINT CLEARING (RECURSIVE)")
print("="*80)

start_time = time.time()
deleted_count = 0
failed_count = 0

try:
    items = dbutils.fs.ls(CHECKPOINT_PATH)
    
    # Separate directories and files
    dirs = []
    files = []
    for item in items:
        is_dir = False
        try:
            is_dir = item.isDir()
        except (AttributeError, TypeError):
            try:
                is_dir = item.isDir
            except AttributeError:
                is_dir = item.path.endswith('/') or '.' not in item.name
        
        if is_dir:
            dirs.append(item.path)
        else:
            files.append(item.path)
    
    print(f"📊 Found {len(dirs)} top-level directories\n")
    
    if dirs:
        # Recursive function to collect all leaf directories
        def collect_all_subdirs(dir_path, current_level=1, max_level=3):
            """Recursively collect all subdirectories up to max_level."""
            subdirs = []
            try:
                items = dbutils.fs.ls(dir_path)
                dir_items = []
                
                # Find all directories at this level
                for item in items:
                    is_dir = False
                    try:
                        is_dir = item.isDir()
                    except (AttributeError, TypeError):
                        try:
                            is_dir = item.isDir
                        except AttributeError:
                            is_dir = item.path.endswith('/') or '.' not in item.name
                    
                    if is_dir:
                        dir_items.append(item.path)
                
                # If we haven't reached max level and found directories, recurse
                if current_level < max_level and dir_items:
                    for subdir in dir_items:
                        # Recurse into this directory
                        nested = collect_all_subdirs(subdir, current_level + 1, max_level)
                        if nested:
                            # This dir has subdirs - add the nested ones
                            subdirs.extend(nested)
                        else:
                            # This dir has no subdirs - it's a leaf, add it
                            subdirs.append(subdir)
                else:
                    # At max level or no subdirs - add all found directories
                    subdirs.extend(dir_items)
                    
            except Exception:
                # If can't list, add this dir for deletion
                subdirs.append(dir_path)
            
            return subdirs
        
        print(f"🔍 Recursively scanning (max 3 levels deep)...")
        
        all_nested_dirs = []
        for top_dir in dirs:
            top_name = top_dir.rstrip('/').split('/')[-1]
            nested = collect_all_subdirs(top_dir, current_level=1, max_level=3)
            print(f"   {top_name}/: {len(nested)} leaf directories")
            all_nested_dirs.extend(nested)
        
        print(f"\n   Total: {len(all_nested_dirs)} directories to delete")
        print(f"\n🔥 Deleting in parallel (20 workers)...")
        
        def delete_subdir(path):
            try:
                dbutils.fs.rm(path, True)
                # Show relative path from checkpoint
                parts = path.rstrip('/').split('/')
                # Find checkpoint root and show path after it
                try:
                    checkpoint_idx = parts.index('_checkpoints')
                    relative_parts = parts[checkpoint_idx+2:]  # Skip checkpoints/usertable
                    display = '/'.join(relative_parts) if relative_parts else parts[-1]
                except (ValueError, IndexError):
                    display = '/'.join(parts[-3:]) if len(parts) >= 3 else parts[-1]
                
                print(f"   ✅ {display}")
                return True
            except Exception as e:
                parts = path.rstrip('/').split('/')
                display = '/'.join(parts[-3:]) if len(parts) >= 3 else parts[-1]
                print(f"   ❌ {display}: {e}")
                return False
        
        with ThreadPoolExecutor(max_workers=20) as executor:
            results = list(executor.map(delete_subdir, all_nested_dirs))
            deleted_count = sum(results)
            failed_count = len([r for r in results if not r])
        
        print(f"\n   ✅ Deleted {deleted_count} directories")
        if failed_count > 0:
            print(f"   ⚠️  Failed: {failed_count}")
    
    # Delete remaining files
    if files:
        print(f"\n🗑️  Deleting {len(files)} files...")
        for file_path in files:
            try:
                dbutils.fs.rm(file_path, False)
                deleted_count += 1
            except:
                failed_count += 1
    
    # Delete parent
    print(f"\n🧹 Cleaning up parent...")
    try:
        dbutils.fs.rm(CHECKPOINT_PATH, True)
        print(f"   ✅ Done")
    except Exception as e:
        print(f"   ℹ️  {e}")
        
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


