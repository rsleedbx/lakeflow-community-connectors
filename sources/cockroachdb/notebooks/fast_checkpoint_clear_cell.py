"""
Copy this cell into your notebook to replace slow checkpoint clearing.

BEFORE (Slow - 5-10 minutes):
    dbutils.fs.rm(f"{CHECKPOINT_PATH}/delta", True)
    spark.sql(f"DROP TABLE IF EXISTS {TARGET_TABLE_PATH}")

AFTER (Fast - 10-60 seconds):
    Use this cell instead!
"""

# ============================================================================
# FAST CHECKPOINT CLEARING (Parallel Deletion)
# ============================================================================

from cockroachdb import parallel_delete_checkpoint

print("="*80)
print("FAST CHECKPOINT CLEARING")
print("="*80)

# Delete checkpoint directory in parallel (5-20x faster!)
result = parallel_delete_checkpoint(
    checkpoint_path=CHECKPOINT_PATH,
    dbutils=dbutils,  # Pass notebook's dbutils context
    max_workers=20,   # Number of parallel threads
    debug=True        # Show progress
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


