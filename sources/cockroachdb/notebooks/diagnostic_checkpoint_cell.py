"""
Add this cell to your notebook BEFORE the checkpoint clearing cell
to diagnose why clearing is slow.
"""

# ============================================================================
# CHECKPOINT DIAGNOSTICS - Run this BEFORE clearing
# ============================================================================

import time

print("="*80)
print("CHECKPOINT DIAGNOSTICS")
print("="*80)
print(f"Checkpoint path: {CHECKPOINT_PATH}")
print(f"Target table: {TARGET_TABLE_PATH}")
print()

# 1. Check checkpoint directory size
print("1️⃣ Analyzing checkpoint directory...")
try:
    delta_path = f"{CHECKPOINT_PATH}/delta"
    files = dbutils.fs.ls(delta_path)
    file_count = len(files)
    total_size = sum(f.size for f in files)
    
    print(f"   📁 Files: {file_count:,}")
    print(f"   💾 Size: {total_size / (1024**2):.2f} MB")
    print(f"   📊 Avg file size: {(total_size/file_count)/(1024):.2f} KB")
    
    # Show file breakdown
    if file_count > 0:
        parquet_files = [f for f in files if f.name.endswith('.parquet')]
        crc_files = [f for f in files if f.name.endswith('.crc')]
        other_files = [f for f in files if not f.name.endswith(('.parquet', '.crc'))]
        
        print(f"   - Parquet files: {len(parquet_files)}")
        print(f"   - CRC files: {len(crc_files)}")
        print(f"   - Other files: {len(other_files)}")
    
    # Warning thresholds
    if file_count > 10000:
        print(f"\n   🔴 CRITICAL: {file_count:,} files! This will be VERY slow!")
        print(f"      Estimated clear time: {file_count * 0.05:.0f}s ({file_count * 0.05 / 60:.1f} min)")
    elif file_count > 1000:
        print(f"\n   🟠 WARNING: {file_count:,} files may cause slow cleanup")
        print(f"      Estimated clear time: {file_count * 0.02:.0f}s ({file_count * 0.02 / 60:.1f} min)")
    elif file_count > 100:
        print(f"\n   🟡 Note: {file_count} files detected (normal for many test runs)")
        print(f"      Estimated clear time: {file_count * 0.01:.0f}s")
    else:
        print(f"\n   ✅ File count is normal (fast clear expected)")
        
except Exception as e:
    print(f"   ℹ️  No checkpoint exists yet: {e}")

print()

# 2. Check Delta table history
print("2️⃣ Analyzing Delta table...")
try:
    history = spark.sql(f"DESCRIBE HISTORY {TARGET_TABLE_PATH}")
    version_count = history.count()
    
    # Get table stats
    detail = spark.sql(f"DESCRIBE DETAIL {TARGET_TABLE_PATH}").first()
    num_files = detail['numFiles'] if hasattr(detail, 'numFiles') else 'N/A'
    size_bytes = detail['sizeInBytes'] if hasattr(detail, 'sizeInBytes') else 0
    
    print(f"   📚 Versions: {version_count:,}")
    print(f"   📁 Data files: {num_files}")
    print(f"   💾 Table size: {size_bytes / (1024**2):.2f} MB")
    
    # Show version breakdown
    if version_count > 0:
        latest = history.orderBy("version", ascending=False).first()
        print(f"   📅 Latest: v{latest['version']} at {latest['timestamp']}")
        print(f"   ⚙️  Operation: {latest['operation']}")
    
    # Warning thresholds
    if version_count > 1000:
        print(f"\n   🔴 CRITICAL: {version_count:,} versions! DROP TABLE will be slow!")
        print(f"      Consider VACUUM to reduce")
    elif version_count > 100:
        print(f"\n   🟠 WARNING: {version_count} versions may slow down DROP")
    else:
        print(f"\n   ✅ Version count is normal")
    
except Exception as e:
    print(f"   ℹ️  Table doesn't exist yet: {e}")

print()

# 3. Estimate clearing time
print("3️⃣ Estimated clearing time...")
try:
    # Get file count again for estimate
    files = dbutils.fs.ls(f"{CHECKPOINT_PATH}/delta")
    file_count = len(files)
    
    # Estimate based on file count (rough estimate: 0.01-0.05s per file on Unity Catalog)
    estimated_seconds = file_count * 0.03  # Conservative estimate
    
    print(f"   ⏱️  Checkpoint clear: {estimated_seconds:.1f}s ({estimated_seconds/60:.1f} min)")
    print(f"   ⏱️  DROP TABLE: 5-30s (depends on versions)")
    print(f"   ⏱️  TOTAL: {estimated_seconds + 15:.1f}s ({(estimated_seconds + 15)/60:.1f} min)")
    
    if estimated_seconds > 300:  # 5 minutes
        print(f"\n   ⚠️  This will take a while! Consider alternatives:")
        print(f"      - Use timestamped checkpoints (no clearing needed)")
        print(f"      - Clear less frequently")
        print(f"      - Use dbutils.fs.rm with smaller batches")
        
except Exception as e:
    print(f"   ℹ️  Cannot estimate: {e}")

print()

# 4. Suggestions
print("💡 Recommendations:")
try:
    files = dbutils.fs.ls(f"{CHECKPOINT_PATH}/delta")
    file_count = len(files)
    
    if file_count > 1000:
        print("   1. Use timestamped checkpoints for each test run:")
        print("      CHECKPOINT_PATH = f'{VOLUME_PATH}/_checkpoints/{SOURCE_TABLE}_{timestamp}'")
        print()
        print("   2. Or clear checkpoints less frequently")
        print()
        print("   3. Or manually clean up old checkpoints:")
        print(f"      dbutils.fs.rm('{CHECKPOINT_PATH}', True)")
    elif file_count > 100:
        print("   Consider using timestamped checkpoints to avoid repeated clearing")
    else:
        print("   ✅ Current checkpoint size is fine")
        
except:
    print("   ✅ No checkpoint exists (first run)")

print("="*80)

# Optional: Pause before clearing
# input("Press Enter to continue with checkpoint clearing...")


