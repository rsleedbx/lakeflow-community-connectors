# Iterator DELETE Operation Filter Fix

## Problem

**Iterator Pattern** was writing **10,550 rows** while **Autoloader Pattern** was writing **9,950 rows** from the same source files.

## Root Cause

`analyze_volume_changefeed_files` analysis shows:
- Total events: 20,700 (fragments)
- After merge: 10,050 events 
- **Active keys: 9,950** ← After applying DELETEs
- Operations: SNAPSHOT=9,500, INSERT=50, UPDATE=400, DELETE=100

**The difference:**
- **Autoloader** applies DELETE operations and **removes** deleted rows from Delta table → 9,950 rows
- **Iterator** was writing **all** CDC records including DELETE operations → 10,550 rows

Iterator count (before fix):
- SNAPSHOT: 10,050 (includes 50 INSERTs converted to SNAPSHOT)
- UPDATE: 400
- DELETE: 100 ← **These should be filtered out!**
- **Total: 10,550**

Autoloader count (correct):
- SNAPSHOT: 9,550 (9,500 + 50 INSERTs)
- UPDATE: 400
- DELETE: 0 (deleted rows are removed)
- **Total: 9,950**

## Solution

Filter out DELETE operations before writing to Delta table in the Iterator pattern:

### Before (Incorrect):
```python
# Write to Delta table
df_merged.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(iterator_table_path)

print(f"✅ Successfully wrote {df_merged.count():,} rows")
```

### After (Correct):
```python
# Filter out DELETE operations to match Autoloader behavior
print(f"\n🔧 Filtering out DELETE operations...")
df_filtered = df_merged.filter("_cdc_operation != 'DELETE'")
deleted_count = df_merged.count() - df_filtered.count()
print(f"   Removed {deleted_count} DELETE records")
print(f"   Final row count: {df_filtered.count():,}")

# Write to Delta table
df_filtered.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(iterator_table_path)

print(f"✅ Successfully wrote {df_filtered.count():,} rows")
```

## Expected Result After Fix

Both patterns should now show:
```
✅ Autoloader Pattern: 9,950 rows
✅ Iterator Pattern: 9,950 rows
✅✅✅ PERFECT MATCH!
```

## Why This is Correct

In CDC processing, DELETE operations represent rows that should be **removed** from the target table. When building a snapshot of the current state:

1. **SNAPSHOT records**: Initial state
2. **INSERT records**: New rows (converted to SNAPSHOT in some systems)
3. **UPDATE records**: Modified rows
4. **DELETE records**: Rows to remove ← Should not be in final table

The Autoloader pattern correctly applies this logic during the merge to Delta. The Iterator pattern should do the same for an apples-to-apples comparison.

## Alternative: Keep DELETE Records

If you want to keep DELETE records for audit/history purposes, you would need a different table design (e.g., Type 2 SCD with soft deletes). But for comparing with Autoloader's behavior, filtering DELETEs is correct.

## Files Modified

- `test_cdc_scenario.ipynb`: Add DELETE filtering before writing Iterator results to Delta

## Verification

After applying the fix:
1. Re-run the Iterator cell
2. Re-run the comparison cell
3. Should see: `✅✅✅ MATCH! Both patterns produced 9,950 rows`
