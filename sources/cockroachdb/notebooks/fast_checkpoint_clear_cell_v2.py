"""
FAST CHECKPOINT CLEARING (No Reload Required)

Copy this cell into your notebook to replace slow checkpoint clearing.

BEFORE (Slow - 5-10 minutes):
    dbutils.fs.rm(f"{CHECKPOINT_PATH}/delta", True)
    spark.sql(f"DROP TABLE IF EXISTS {TARGET_TABLE_PATH}")

AFTER (Fast - 10-60 seconds):
    Use this cell!
"""

# ============================================================================
# FAST CHECKPOINT CLEARING (Parallel Deletion)
# ============================================================================

# Import directly (no reload needed if function is already in cockroachdb.py)
try:
    from cockroachdb import parallel_delete_checkpoint
except ImportError:
    # If import fails, use local implementation
    print("⚠️  Using fallback - please restart kernel to use cockroachdb.parallel_delete_checkpoint")
    
    # Fallback: inline parallel delete function
    def parallel_delete_checkpoint(checkpoint_path, dbutils, max_workers=20, debug=True):
        import time
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        if debug:
            print("=" * 80)
            print("PARALLEL CHECKPOINT DELETION (FALLBACK)")
            print("=" * 80)
            print(f"Path: {checkpoint_path}")
            print(f"Workers: {max_workers}")
            print()
        
        start_time = time.time()
        deleted_count = 0
        failed_count = 0
        
        try:
            items = dbutils.fs.ls(checkpoint_path)
        except:
            if debug:
                print(f"ℹ️  Checkpoint doesn't exist: {checkpoint_path}")
            return {
                'success': True,
                'deleted_count': 0,
                'failed_count': 0,
                'elapsed_seconds': time.time() - start_time,
                'subdirs': []
            }
        
        # Separate directories and files - handle different FileInfo APIs
        dirs = []
        files = []
        for item in items:
            is_dir = False
            try:
                is_dir = item.isDir()  # Method call
            except (AttributeError, TypeError):
                try:
                    is_dir = item.isDir  # Property
                except AttributeError:
                    is_dir = item.path.endswith('/') or '.' not in item.name
            
            if is_dir:
                dirs.append(item.path)
            else:
                files.append(item.path)
        
        if debug:
            print(f"📊 Found: {len(dirs)} subdirectories, {len(files)} files\n")
        
        if dirs:
            if debug:
                print(f"🔥 Deleting {len(dirs)} subdirectories in parallel...")
            
            def delete_subdir(path):
                try:
                    dbutils.fs.rm(path, True)
                    return {'success': True, 'name': path.split('/')[-1]}
                except Exception as e:
                    return {'success': False, 'name': path.split('/')[-1], 'error': str(e)}
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [executor.submit(delete_subdir, dir_path) for dir_path in dirs]
                for future in as_completed(futures):
                    result = future.result()
                    if result['success']:
                        deleted_count += 1
                    else:
                        failed_count += 1
            
            if debug:
                print(f"   ✅ Deleted {deleted_count} subdirectories\n")
        
        # Delete files
        for file_path in files:
            try:
                dbutils.fs.rm(file_path, False)
                deleted_count += 1
            except:
                failed_count += 1
        
        # Delete parent
        try:
            dbutils.fs.rm(checkpoint_path, True)
        except:
            pass
        
        elapsed = time.time() - start_time
        
        if debug:
            print(f"✅ Deleted {deleted_count} items in {elapsed:.1f}s")
            if len(dirs) > 100:
                speedup = (len(dirs) * 0.5) / elapsed if elapsed > 0 else 1
                print(f"⚡ Estimated speedup: {speedup:.1f}x faster")
        
        return {
            'success': failed_count == 0,
            'deleted_count': deleted_count,
            'failed_count': failed_count,
            'elapsed_seconds': elapsed,
            'subdirs': []
        }

print("="*80)
print("FAST CHECKPOINT CLEARING")
print("="*80)

# Delete checkpoint directory in parallel
result = parallel_delete_checkpoint(
    checkpoint_path=CHECKPOINT_PATH,
    dbutils=dbutils,
    max_workers=20,
    debug=True
)

# Drop the Delta table
try:
    spark.sql(f"DROP TABLE IF EXISTS {TARGET_TABLE_PATH}")
    print(f"\n✅ Dropped table: {TARGET_TABLE_PATH}")
except Exception as e:
    print(f"ℹ️  Table drop: {e}")

print()
print("="*80)
print("CLEAR COMPLETE")
print("="*80)
print(f"⏱️  Total time: {result['elapsed_seconds']:.1f}s")
print(f"📊 Items deleted: {result['deleted_count']}")

if result['failed_count'] > 0:
    print(f"⚠️  Failed deletions: {result['failed_count']}")
    
print("="*80)

