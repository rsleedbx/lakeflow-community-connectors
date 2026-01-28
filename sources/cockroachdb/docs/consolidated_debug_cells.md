# Consolidated Debug Section

Replace the content under `# Debug Codes` markdown with these 3 cells:

---

## Cell 1: Quick Missing Keys Check (Lightweight)

```python
# ============================================================================
# DEBUG CELL 1: Quick Missing Keys Check
# ============================================================================
# Lightweight check to see if specific keys exist in CockroachDB and staging
# Update missing_keys list based on Cell 14/15 output
# ============================================================================

target_table_fqn = f"{target_catalog}.{target_schema}.{target_table}"
staging_table_cf = f"{target_table_fqn}_staging_cf"
missing_keys = [17, 18, 19]  # ← Update this based on Cell 14/15 output

print("🔍 Quick Debug: Checking missing keys...")
print("=" * 80)

# Check CockroachDB
print(f"\n📊 CockroachDB ({source_table}):")
with get_cockroachdb_connection() as conn:
    cursor = conn.cursor()
    for key in missing_keys:
        cursor.execute(f"SELECT * FROM {source_table} WHERE ycsb_key = %s", (key,))
        result = cursor.fetchone()
        print(f"   Key {key}: {'✅ EXISTS' if result else '❌ NOT FOUND (deleted)'}")

# Check Staging Table
print(f"\n📊 Staging Table ({staging_table_cf}):")
if spark.catalog.tableExists(staging_table_cf):
    staging_df = spark.read.table(staging_table_cf)
    for key in missing_keys:
        count = staging_df.filter(F.col("ycsb_key") == key).count()
        print(f"   Key {key}: {count} row(s)")
    
    print("\n💡 Next steps:")
    print("   - If keys exist in CockroachDB but not in staging:")
    print("     → Re-run Cell 12 to pick up new CDC files")
    print("   - If keys exist in staging but not in target:")
    print("     → Check Cell 12 output for MERGE errors")
    print("     → Run DEBUG CELL 2 for detailed analysis")
else:
    print("   ⚠️  Staging table doesn't exist (Cell 12 dropped it)")
    print("   💡 Re-run Cell 12 to recreate staging for debugging")
```

---

## Cell 2: Inspect Target Table (Comprehensive Analysis)

```python
# ============================================================================
# DEBUG CELL 2: Inspect Target Table
# ============================================================================
# Comprehensive analysis of target table:
# - CDC operation distribution
# - Key distribution and gaps
# - Duplicate detection
# - Sample records
# 
# Use this to diagnose sync issues and data quality problems
# ============================================================================

target_table_fqn = f"{target_catalog}.{target_schema}.{target_table}"

print(f"📊 Target Table Analysis: {target_table_fqn}")
print("=" * 80)

# Read target table
df = spark.read.table(target_table_fqn)
total_rows = df.count()
print(f"\n📈 Total Rows: {total_rows:,}")

# Group by CDC operation type
if "_cdc_operation" in df.columns:
    print("\n🔍 CDC Operations:")
    df.groupBy("_cdc_operation").count().orderBy("_cdc_operation").show()
    
    # Check if DELETE rows are stored as data (should NOT happen)
    delete_count = df.filter(F.col("_cdc_operation") == "DELETE").count()
    if delete_count > 0:
        print(f"\n⚠️  WARNING: Found {delete_count} DELETE rows stored as data!")
        print("   This is a bug - DELETE rows should not be in the target table.")
        print("   💡 Run Cell 16 to fix (drops and recreates table)")

# Show key distribution
if "ycsb_key" in df.columns:
    print("\n🔍 Key Distribution:")
    key_dist = df.groupBy("ycsb_key").count().orderBy("ycsb_key")
    key_dist.show(50)
    
    # Check for duplicates
    duplicates = key_dist.filter("count > 1")
    dup_count = duplicates.count()
    if dup_count > 0:
        print(f"\n⚠️  Found {dup_count} duplicate keys!")
        duplicates.show()
        print("\n   💡 This indicates deduplication failure in MERGE logic")
    else:
        print("\n✅ No duplicate keys found")
    
    # Show key range and gaps
    key_stats = df.select(
        F.min("ycsb_key").alias("min_key"),
        F.max("ycsb_key").alias("max_key"),
        F.count("ycsb_key").alias("count")
    ).collect()[0]
    
    expected_count = key_stats["max_key"] - key_stats["min_key"] + 1
    actual_count = key_stats["count"]
    missing_count = expected_count - actual_count
    
    print(f"\n📊 Key Range Analysis:")
    print(f"   Min key: {key_stats['min_key']}")
    print(f"   Max key: {key_stats['max_key']}")
    print(f"   Expected rows (if contiguous): {expected_count}")
    print(f"   Actual rows: {actual_count}")
    
    if missing_count > 0:
        print(f"   ⚠️  Missing {missing_count} keys (gaps in range)")
        print(f"\n   💡 Run DEBUG CELL 1 or CELL 3 to investigate specific keys")
    else:
        print(f"   ✅ No gaps (keys are contiguous)")

# Show sample records
print("\n🔍 Sample Records (ordered by key):")
df.orderBy("ycsb_key").show(30, truncate=False)

print("\n" + "=" * 80)
print("💡 If you see issues:")
print("   - DELETE rows stored as data → Run Cell 16 (recreate table)")
print("   - Duplicate keys → Check MERGE deduplication logic")
print("   - Missing keys → Run DEBUG CELL 1 or CELL 3")
print("   - Gaps in key range → Keys were deleted (normal for update-delete mode)")
```

---

## Cell 3: Detailed Missing Keys Investigation

```python
# ============================================================================
# DEBUG CELL 3: Detailed Missing Keys Investigation
# ============================================================================
# Detailed investigation of missing keys:
# - Checks CockroachDB source
# - Checks staging table (if it exists)
# - Shows CDC operation and timestamps
# - Provides detailed troubleshooting steps
#
# Update missing_keys list based on Cell 14/15 or DEBUG CELL 2 output
# ============================================================================

target_table_fqn = f"{target_catalog}.{target_schema}.{target_table}"
staging_table_cf = f"{target_table_fqn}_staging_cf"

# Update this list based on what's missing
missing_keys = [17, 18, 19]  # ← Update based on DEBUG CELL 2 output

print("🔍 Detailed Missing Keys Investigation")
print("=" * 80)
print(f"Investigating keys: {missing_keys}")
print()

# ============================================================================
# Step 1: Check CockroachDB Source
# ============================================================================
print("📊 STEP 1: Checking CockroachDB Source")
print("-" * 80)

with get_cockroachdb_connection() as conn:
    cursor = conn.cursor()
    
    for key in missing_keys:
        cursor.execute(f"SELECT * FROM {source_table} WHERE ycsb_key = %s", (key,))
        result = cursor.fetchone()
        
        if result:
            print(f"✅ Key {key}: EXISTS in CockroachDB")
            # Show first 3 fields for verification
            print(f"   Sample data: {result[:min(3, len(result))]}")
        else:
            print(f"❌ Key {key}: NOT FOUND in CockroachDB")
            print(f"   → This key was deleted (expected for update-delete mode)")

# ============================================================================
# Step 2: Check Staging Table
# ============================================================================
print(f"\n📊 STEP 2: Checking Staging Table")
print("-" * 80)

if spark.catalog.tableExists(staging_table_cf):
    staging_df = spark.read.table(staging_table_cf)
    print(f"✅ Staging table exists: {staging_table_cf}")
    print()
    
    for key in missing_keys:
        key_rows = staging_df.filter(F.col("ycsb_key") == key)
        count = key_rows.count()
        
        if count > 0:
            print(f"✅ Key {key}: {count} row(s) in staging table")
            print("   Details:")
            key_rows.select(
                "ycsb_key", 
                "_cdc_timestamp", 
                "_cdc_operation", 
                "field0", 
                "field1"
            ).show(truncate=False)
        else:
            print(f"❌ Key {key}: NOT in staging table")
    
    # Show staging table summary
    print("\n📈 Staging Table Summary:")
    staging_df.groupBy("_cdc_operation").count().show()
    
else:
    print(f"⚠️  Staging table doesn't exist: {staging_table_cf}")
    print("   This means Cell 12 completed and dropped the staging table")
    print("\n💡 To debug further:")
    print("   1. Re-run Cell 12 (it will process new files and recreate staging)")
    print("   2. Run this cell again to check staging table")

# ============================================================================
# Step 3: Troubleshooting Recommendations
# ============================================================================
print("\n" + "=" * 80)
print("💡 TROUBLESHOOTING GUIDE")
print("=" * 80)

print("\n📋 If keys EXIST in CockroachDB but NOT in staging:")
print("   → CDC files haven't been picked up by Auto Loader yet")
print("   ✅ Solution: Re-run Cell 12 to process new CDC files")

print("\n📋 If keys EXIST in staging but NOT in target:")
print("   → MERGE logic failed or conditions are wrong")
print("   ✅ Solution: Check Cell 12 output for MERGE errors")
print("   ✅ Alternative: Check MERGE conditions in Cell 6")

print("\n📋 If keys DON'T EXIST in CockroachDB:")
print("   → Keys were deleted (normal for update-delete mode)")
print("   ✅ Expected: Target should also not have these keys")
print("   ⚠️  If target HAS these keys: MERGE delete logic isn't working")

print("\n📋 If keys DON'T EXIST anywhere:")
print("   → Keys were never created, or CDC didn't capture them")
print("   ✅ Check: Run Cell 10 again to verify workload ran correctly")
```

---

## Instructions

1. **Navigate to the `# Debug Codes` markdown section** in your notebook
2. **Delete any existing code cells** under that section
3. **Insert these 3 cells** in order:
   - Cell 1: Quick check (fast, for common cases)
   - Cell 2: Comprehensive analysis (detailed table inspection)
   - Cell 3: Deep investigation (when you need full details)

4. **Usage Guide:**
   - **Start with Cell 1** if you know specific keys are missing
   - **Use Cell 2** for general table health check
   - **Use Cell 3** for deep investigation of specific issues

All cells are independent and can be run in any order!
