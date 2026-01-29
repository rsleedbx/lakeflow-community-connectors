# Notebook Refactoring Guide

This guide shows how to refactor `cockroachdb-cdc-tutorial.ipynb` to use the new `cockroachdb_ycsb.py` module.

## Summary of Changes

**Total cells to modify**: 3 cells (Cell 6, Cell 7, Cell 9)

**Benefit**: Reduces notebook code by ~150 lines while making YCSB functions reusable across projects.

---

## Cell 6: Create Table

### Before (59 lines):
```python
# Create table structure based on column_family_mode:
# - single_cf: 1 column family (default, better performance)
# - multi_cf: 3 column families (for testing split_column_families=true)

if column_family_mode == "multi_cf":
    # Create table with MULTIPLE column families for testing split_column_families=true
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {source_table} (
        ycsb_key INT PRIMARY KEY,
        -- Family 1: Frequently accessed fields
        field0 TEXT,
        field1 TEXT,
        field2 TEXT,
        FAMILY frequently_read (ycsb_key, field0, field1, field2),
        
        -- Family 2: Medium-frequency fields
        field3 TEXT,
        field4 TEXT,
        field5 TEXT,
        FAMILY medium_read (field3, field4, field5),
        
        -- Family 3: Rarely accessed fields
        field6 TEXT,
        field7 TEXT,
        field8 TEXT,
        field9 TEXT,
        FAMILY rarely_read (field6, field7, field8, field9)
    )
    """
    family_info = "3 column families (frequently_read, medium_read, rarely_read)"
else:
    # Create table with SINGLE column family (default)
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {source_table} (
        ycsb_key INT PRIMARY KEY,
        field0 TEXT,
        field1 TEXT,
        field2 TEXT,
        field3 TEXT,
        field4 TEXT,
        field5 TEXT,
        field6 TEXT,
        field7 TEXT,
        field8 TEXT,
        field9 TEXT
    )
    """
    family_info = "1 column family (default primary)"

conn = get_cockroachdb_connection()
try:
    with conn.cursor() as cur:
        cur.execute(create_table_sql)
        conn.commit()
    print(f"✅ Table '{source_table}' created (or already exists)")
    print(f"   Column Family Mode: {column_family_mode}")
    print(f"   Column families: {family_info}")
finally:
    conn.close()
```

### After (13 lines):
```python
# Create table using cockroachdb_ycsb.py
import importlib, cockroachdb_ycsb
importlib.reload(cockroachdb_ycsb)
from cockroachdb_ycsb import create_ycsb_table

conn = get_cockroachdb_connection()
try:
    create_ycsb_table(
        conn=conn,
        table_name=source_table,
        column_family_mode=column_family_mode
    )
finally:
    conn.close()
```

**Savings**: 46 lines

---

## Cell 7: Insert Snapshot Data

### Before (41 lines):
```python
conn = get_cockroachdb_connection()
try:
    # Check if table is empty using helper function
    stats = get_table_stats(conn, source_table)
    
    if stats['is_empty']:
        # Table is empty - insert snapshot data
        print(f"📊 Table is empty. Inserting {snapshot_count} initial rows (snapshot phase)...")
        
        with conn.cursor() as cur:
            # Use generate_series for efficient bulk insert
            insert_sql = f"""
            INSERT INTO {source_table} 
            (ycsb_key, field0, field1, field2, field3, field4, field5, field6, field7, field8, field9)
            SELECT 
                i AS ycsb_key,
                'snapshot_value_' || i || '_0' AS field0,
                'snapshot_value_' || i || '_1' AS field1,
                'snapshot_value_' || i || '_2' AS field2,
                'snapshot_value_' || i || '_3' AS field3,
                'snapshot_value_' || i || '_4' AS field4,
                'snapshot_value_' || i || '_5' AS field5,
                'snapshot_value_' || i || '_6' AS field6,
                'snapshot_value_' || i || '_7' AS field7,
                'snapshot_value_' || i || '_8' AS field8,
                'snapshot_value_' || i || '_9' AS field9
            FROM generate_series(0, %s - 1) AS i
            """
            
            cur.execute(insert_sql, (snapshot_count,))
            conn.commit()
        
        print(f"✅ Sample data inserted using generate_series")
        print(f"   Rows inserted: {snapshot_count} (keys 0 to {snapshot_count - 1})")
    else:
        # Table already has data - skip insert
        print(f"ℹ️  Table already contains data - skipping snapshot insert")
        print(f"   Current key range: {stats['min_key']} to {stats['max_key']}")
        print(f"   Tip: If you want to re-run the snapshot, drop the table first (see Cleanup cells)")
finally:
    conn.close()
```

### After (11 lines):
```python
# Insert snapshot data using cockroachdb_ycsb.py
from cockroachdb_ycsb import insert_ycsb_snapshot

conn = get_cockroachdb_connection()
try:
    insert_ycsb_snapshot(
        conn=conn,
        table_name=source_table,
        snapshot_count=snapshot_count
    )
finally:
    conn.close()
```

**Savings**: 30 lines

---

## Cell 9: Run Workload

### Before (102 lines, including Azure file wait logic):
```python
import time
from datetime import datetime

# Capture baseline file count BEFORE generating CDC events
print("📊 Capturing baseline file count...")
result_before = check_azure_files(
    storage_account_name, storage_account_key, container_name,
    source_catalog, source_schema, source_table, target_table,
    verbose=False
)
files_before = len(result_before['data_files'])
print(f"   Current files: {files_before}")
print()

conn = get_cockroachdb_connection()
try:
    # Get current table state using helper function
    stats_before = get_table_stats(conn, source_table)
    min_key = stats_before['min_key']
    max_key = stats_before['max_key']
    count_before = stats_before['count']
    
    print(f"📊 Current table state:")
    print(f"   Min key: {min_key}, Max key: {max_key}, Total rows: {count_before}")
    print()
    
    with conn.cursor() as cur:
        
        # 1. INSERT: Add new rows starting from max_key + 1 (using generate_series)
        print(f"➕ Running {insert_count} INSERTs (keys {max_key + 1} to {max_key + insert_count})...")
        insert_sql = f"""
        INSERT INTO {source_table} 
        (ycsb_key, field0, field1, field2, field3, field4, field5, field6, field7, field8, field9)
        SELECT 
            i AS ycsb_key,
            'inserted_value_' || i || '_0' AS field0,
            'inserted_value_' || i || '_1' AS field1,
            'inserted_value_' || i || '_2' AS field2,
            'inserted_value_' || i || '_3' AS field3,
            'inserted_value_' || i || '_4' AS field4,
            'inserted_value_' || i || '_5' AS field5,
            'inserted_value_' || i || '_6' AS field6,
            'inserted_value_' || i || '_7' AS field7,
            'inserted_value_' || i || '_8' AS field8,
            'inserted_value_' || i || '_9' AS field9
        FROM generate_series(%s, %s) AS i
        """
        cur.execute(insert_sql, (max_key + 1, max_key + insert_count))
        
        # 2. UPDATE: Update existing rows starting from min_key (single UPDATE statement)
        print(f"📝 Running {update_count} UPDATEs (keys {min_key} to {min_key + update_count - 1})...")
        timestamp = int(time.time())
        cur.execute(f"""
            UPDATE {source_table}
            SET field0 = %s
            WHERE ycsb_key >= %s AND ycsb_key < %s
        """, (f"updated_at_{timestamp}", min_key, min_key + update_count))
        
        # 3. DELETE: Delete oldest rows starting from min_key (single DELETE)
        delete_max = min_key + delete_count - 1
        print(f"🗑️  Running {delete_count} DELETEs (keys {min_key} to {delete_max})...")
        cur.execute(f"""
            DELETE FROM {source_table}
            WHERE ycsb_key >= %s AND ycsb_key <= %s
        """, (min_key, delete_max))
        
        conn.commit()
    
    # Get final table state using helper function
    stats_after = get_table_stats(conn, source_table)
    min_key_after = stats_after['min_key']
    max_key_after = stats_after['max_key']
    count_after = stats_after['count']
    
    print(f"\n✅ Workload complete")
    print(f"   Inserts: {insert_count}")
    print(f"   Updates: {update_count}")
    print(f"   Deletes: {delete_count}")
    print(f"   Before: {count_before} rows (keys {min_key}-{max_key})")
    print(f"   After:  {count_after} rows (keys {min_key_after}-{max_key_after})")
    print(f"   Net change: {count_after - count_before:+d} rows")
    print(f"")
    
    # Wait for new CDC files to appear in Azure (positive confirmation)
    print(f"⏳ Waiting for new CDC files to appear in Azure...")
    print(f"   Baseline: {files_before} files")
    print()
    
    # Poll for new files (max 90 seconds)
    max_wait = 90
    check_interval = 10
    elapsed = 0
    
    while elapsed < max_wait:
        result = check_azure_files(
            storage_account_name, storage_account_key, container_name,
            source_catalog, source_schema, source_table, target_table,
            verbose=False
        )
        files_now = len(result['data_files'])
        
        if files_now > files_before:
            print(f"✅ New CDC files appeared after {elapsed} seconds!")
            print(f"   Baseline (before workload): {files_before} files")
            print(f"   Current (after workload): {files_now} files")
            print(f"   New files generated: {files_now - files_before}")
            break
        
        print(f"   Checking... ({elapsed}s elapsed, baseline: {files_before} files)", end='\r')
        time.sleep(check_interval)
        elapsed += check_interval
    else:
        print(f"\n⚠️  Timeout after {max_wait}s - files may still be flushing")
        print(f"   Run Cell 11 to check manually")
finally:
    conn.close()
```

### After (46 lines):
```python
import time

# Capture baseline file count BEFORE generating CDC events
print("📊 Capturing baseline file count...")
result_before = check_azure_files(
    storage_account_name, storage_account_key, container_name,
    source_catalog, source_schema, source_table, target_table,
    verbose=False
)
files_before = len(result_before['data_files'])
print(f"   Current files: {files_before}")
print()

# Run workload using cockroachdb_ycsb.py
from cockroachdb_ycsb import run_ycsb_workload

conn = get_cockroachdb_connection()
try:
    run_ycsb_workload(
        conn=conn,
        table_name=source_table,
        insert_count=insert_count,
        update_count=update_count,
        delete_count=delete_count
    )
finally:
    conn.close()

# Wait for new CDC files to appear in Azure (positive confirmation)
print(f"")
print(f"⏳ Waiting for new CDC files to appear in Azure...")
print(f"   Baseline: {files_before} files")
print()

# Poll for new files (max 90 seconds)
max_wait = 90
check_interval = 10
elapsed = 0

while elapsed < max_wait:
    result = check_azure_files(
        storage_account_name, storage_account_key, container_name,
        source_catalog, source_schema, source_table, target_table,
        verbose=False
    )
    files_now = len(result['data_files'])
    
    if files_now > files_before:
        print(f"✅ New CDC files appeared after {elapsed} seconds!")
        print(f"   Baseline (before workload): {files_before} files")
        print(f"   Current (after workload): {files_now} files")
        print(f"   New files generated: {files_now - files_before}")
        break
    
    print(f"   Checking... ({elapsed}s elapsed, baseline: {files_before} files)", end='\r')
    time.sleep(check_interval)
    elapsed += check_interval
else:
    print(f"\n⚠️  Timeout after {max_wait}s - files may still be flushing")
    print(f"   Run Cell 11 to check manually")
```

**Savings**: 56 lines

---

## Total Impact

- **Lines removed**: ~150 lines of inline code
- **Lines added**: ~70 lines of function calls + imports
- **Net reduction**: ~80 lines in notebook
- **Reusability**: YCSB functions now available for all CDC testing projects

---

## Testing Instructions

After making these changes:

1. **Run Cell 6** - Should see same output as before
2. **Run Cell 7** - Should see same output as before  
3. **Run Cell 9** - Should see same output as before
4. **Run Cell 13** - Verify sync still works

The output and behavior should be **100% identical** to the original notebook.

---

## Optional: Add to Cell 5

For convenience, you can add these imports to Cell 5 (after the existing Azure functions):

```python
# Import YCSB table management functions
import importlib, cockroachdb_ycsb
importlib.reload(cockroachdb_ycsb)
from cockroachdb_ycsb import (
    create_ycsb_table,
    insert_ycsb_snapshot,
    run_ycsb_workload
)
print("✅ YCSB functions imported from cockroachdb_ycsb.py")
```

Then you can remove the `import` lines from Cells 6, 7, and 9.

---

## Rollback Plan

If something goes wrong, you can easily revert by copying the "Before" code back into each cell. All the original logic is preserved in `cockroachdb_ycsb.py` - we're just calling it instead of inlining it.
