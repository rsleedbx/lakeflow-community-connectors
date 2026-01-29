# CockroachDB CDC Debug Utilities

## Overview

`cockroachdb_debug.py` provides comprehensive debugging tools for investigating CDC pipeline issues, particularly for multi-column family scenarios where column sums don't match between source and target.

## Problem Context

When using `split_column_families=true` in CockroachDB changefeeds, each CDC event may be split into multiple fragments across different column families. If these fragments aren't properly merged, you may see:

- ✅ Row counts match
- ✅ Primary key sums match
- ✅ First column family (e.g., `field0-2`) matches perfectly
- ❌ **Other column families (e.g., `field3-9`) show sum mismatches**

This indicates incomplete column family fragment merging.

## Quick Start

### In Databricks Notebook:

```python
# Import debug utilities
from cockroachdb_debug import (
    diagnose_column_family_sync,
    analyze_cdc_events_by_column_family,
    check_merge_completeness,
    inspect_raw_cdc_files,
    run_full_diagnosis
)

# Run comprehensive diagnosis
conn = get_cockroachdb_connection()
try:
    run_full_diagnosis(
        conn=conn,
        spark=spark,
        source_table="defaultdb.public.usertable_update_delete_multi_cf",
        target_df=target_df,
        staging_table="main.schema.usertable_staging",
        azure_path="abfss://container@account.dfs.core.windows.net/path/",
        primary_keys=['ycsb_key'],
        mismatched_columns=['field3', 'field4', 'field5', 'field6', 'field7', 'field8', 'field9']
    )
finally:
    conn.close()
```

## Available Functions

### 1. `get_column_families(conn, table_name)`

**Purpose:** Retrieve column family assignments from CockroachDB

**Returns:** Dict mapping column family names to lists of columns

**Example:**
```python
families = get_column_families(conn, "usertable_update_delete_multi_cf")
# Output:
# {
#   'pk': ['ycsb_key', 'field0', 'field1', 'field2'],
#   'data1': ['field3', 'field4', 'field5'],
#   'data2': ['field6', 'field7', 'field8', 'field9']
# }
```

**Use Case:** Identify which columns belong to which column family to understand fragmentation patterns.

---

### 2. `compare_row_by_row(conn, source_table, target_df, primary_keys, columns_to_check, limit)`

**Purpose:** Compare specific rows between source and target to identify exact discrepancies

**Arguments:**
- `conn`: CockroachDB connection
- `source_table`: Source table name
- `target_df`: Target Spark DataFrame
- `primary_keys`: List of primary key columns
- `columns_to_check`: Columns to compare (e.g., `['field3', 'field4', ...]`)
- `limit`: Number of rows to compare (default: 10)

**Example:**
```python
compare_row_by_row(
    conn=conn,
    source_table="defaultdb.public.usertable_update_delete_multi_cf",
    target_df=target_df,
    primary_keys=['ycsb_key'],
    columns_to_check=['field3', 'field4', 'field5'],
    limit=5
)
```

**Output:**
```
✅ Key (ycsb_key=80): All columns match
❌ Key (ycsb_key=81):
   field3: 1769625671 vs 1769625644
   field4: 1769625671 vs 1769625644
```

**Use Case:** Identify specific keys and columns with mismatches for targeted debugging.

---

### 3. `analyze_cdc_events_by_column_family(spark, azure_path, table_name, primary_keys)`

**Purpose:** Analyze raw CDC events from Azure to check column family fragment distribution

**Arguments:**
- `spark`: Spark session
- `azure_path`: Path to Azure changefeed files (e.g., `abfss://...`)
- `table_name`: Table name to filter
- `primary_keys`: List of primary key columns

**Example:**
```python
analyze_cdc_events_by_column_family(
    spark=spark,
    azure_path="abfss://changefeed-events@storage.dfs.core.windows.net/parquet/defaultdb/public/usertable/",
    table_name="usertable_update_delete_multi_cf",
    primary_keys=['ycsb_key']
)
```

**Output:**
```
📊 Total CDC events: 12,450

📊 Events by Operation:
+-------------------+-----+
|__crdb__event_type|count|
+-------------------+-----+
|                  c|  450|  (UPDATE)
|                  d|  100|  (DELETE)
|                  i|   50|  (INSERT)
+-------------------+-----+

📊 Column Completeness (NULL count per column):
  ✅ ycsb_key        :      0 NULL (0.0%)
  ✅ field0          :      0 NULL (0.0%)
  ✅ field1          :      0 NULL (0.0%)
  ✅ field2          :      0 NULL (0.0%)
  ⚠️  field3          :  8,250 NULL (66.3%)  ← Column family fragment!
  ⚠️  field4          :  8,250 NULL (66.3%)
  ⚠️  field5          :  8,250 NULL (66.3%)

📊 UPDATE Events Analysis:
  Total UPDATE events: 450
  
  Column completeness in UPDATE events:
    ✅ ycsb_key        :      0 NULL (0.0%)
    ⚠️  field3          :    150 NULL (33.3%)  ← 1/3 of UPDATEs missing field3!
    ⚠️  field4          :    150 NULL (33.3%)

📊 Column Family Fragmentation Check:
  ⚠️  Found 450 keys with multiple fragments
```

**Use Case:** Understand if CDC events are properly fragmented and if all fragments are present.

---

### 4. `check_merge_completeness(spark, staging_table, primary_keys)`

**Purpose:** Check if column family fragments were properly merged in staging table

**Arguments:**
- `spark`: Spark session
- `staging_table`: Fully qualified staging table name
- `primary_keys`: List of primary key columns

**Example:**
```python
check_merge_completeness(
    spark=spark,
    staging_table="main.robert_lee_cockroachdb.usertable_staging",
    primary_keys=['ycsb_key']
)
```

**Output:**
```
📊 Total rows in staging: 10,450

📊 NULL values by column:
  ✅ ycsb_key        :      0 NULL (0.0%)
  ✅ field0          :      0 NULL (0.0%)
  ⚠️  field3          :    150 NULL (1.4%)  ← Merge incomplete!
  ⚠️  field4          :    150 NULL (1.4%)

📊 Duplicate primary key check:
  ✅ No duplicate primary keys (merge successful)
```

**Use Case:** Verify if the MERGE operation correctly combined all column family fragments.

---

### 5. `inspect_raw_cdc_files(spark, azure_path, primary_key, key_value, show_content)`

**Purpose:** Inspect raw CDC files for a specific key to see all fragments

**Arguments:**
- `spark`: Spark session
- `azure_path`: Path to Azure changefeed files
- `primary_key`: Primary key column name
- `key_value`: Specific key value to inspect
- `show_content`: Whether to show full column values (default: True)

**Example:**
```python
inspect_raw_cdc_files(
    spark=spark,
    azure_path="abfss://changefeed-events@storage.dfs.core.windows.net/parquet/defaultdb/public/usertable/",
    primary_key='ycsb_key',
    key_value=80,
    show_content=False
)
```

**Output:**
```
📊 Found 3 CDC events for this key

📄 Events (ordered by timestamp):
+----------+-------------------+-------------------+-------+-------+-------+
|ycsb_key  |__crdb__event_type |__crdb__updated    |field0 |field3 |field6 |
+----------+-------------------+-------------------+-------+-------+-------+
|80        |c                  |1769625671.00000000|...|NULL   |NULL   |
|80        |c                  |1769625671.00000001|NULL   |...    |NULL   |
|80        |c                  |1769625671.00000002|NULL   |NULL   |...    |
+----------+-------------------+-------------------+-------+-------+-------+

📊 Fragment Analysis:
  Fragment 1: c @ 1769625671.00000000 - 3/11 columns populated
  Fragment 2: c @ 1769625671.00000001 - 3/11 columns populated
  Fragment 3: c @ 1769625671.00000002 - 5/11 columns populated
```

**Use Case:** Understand the exact fragmentation pattern for a specific key to debug merge issues.

---

### 6. `diagnose_column_family_sync(conn, source_table, target_df, primary_keys, mismatched_columns)`

**Purpose:** Comprehensive diagnosis of column family sync issues

**Arguments:**
- `conn`: CockroachDB connection
- `source_table`: Source table name
- `target_df`: Target Spark DataFrame
- `primary_keys`: List of primary key columns
- `mismatched_columns`: Columns with sum mismatches (from Cell 14 output)

**Example:**
```python
diagnose_column_family_sync(
    conn=conn,
    source_table="defaultdb.public.usertable_update_delete_multi_cf",
    target_df=target_df,
    primary_keys=['ycsb_key'],
    mismatched_columns=['field3', 'field4', 'field5', 'field6', 'field7', 'field8', 'field9']
)
```

**Output:**
```
1️⃣  Column Family Assignments:

  📁 Family 'pk':
     ✅ ycsb_key
     ✅ field0
     ✅ field1
     ✅ field2

  📁 Family 'data1':
     ❌ field3
     ❌ field4
     ❌ field5

  📁 Family 'data2':
     ❌ field6
     ❌ field7
     ❌ field8
     ❌ field9

2️⃣  Sample Row Comparison:
[Shows specific row-by-row comparisons]

3️⃣  Mismatch Pattern Analysis:
  Analyzing 7 mismatched columns...
  Columns: field3, field4, field5, field6, field7, field8, field9
  
  💡 Hypothesis: Missing UPDATE events for column families
     If some UPDATE events didn't merge properly, those columns
     would retain old values instead of updated values.
```

**Use Case:** Get a complete picture of the column family structure and where mismatches occur.

---

### 7. `run_full_diagnosis(conn, spark, source_table, target_df, staging_table, azure_path, primary_keys, mismatched_columns)`

**Purpose:** Run all diagnostic functions in sequence for comprehensive analysis

**Arguments:** All arguments from the individual functions above

**Example:**
```python
run_full_diagnosis(
    conn=conn,
    spark=spark,
    source_table="defaultdb.public.usertable_update_delete_multi_cf",
    target_df=target_df,
    staging_table="main.robert_lee_cockroachdb.usertable_staging",
    azure_path="abfss://changefeed-events@storage.dfs.core.windows.net/parquet/defaultdb/public/usertable/",
    primary_keys=['ycsb_key'],
    mismatched_columns=['field3', 'field4', 'field5', 'field6', 'field7', 'field8', 'field9']
)
```

**Use Case:** One-stop comprehensive diagnosis - runs all checks in logical order.

---

## Common Debugging Scenarios

### Scenario 1: Field3-9 sum mismatches, field0-2 perfect

**Diagnosis:**
1. Run `get_column_families()` to confirm field0-2 are in first family, field3-9 in others
2. Run `analyze_cdc_events_by_column_family()` to check for NULL values
3. Run `check_merge_completeness()` to verify staging table merge

**Likely Cause:** Column family fragments not being properly merged

**Fix:** Review MERGE logic in Cell 6 (`ingest_cdc_with_merge_multi_family()`)

---

### Scenario 2: Specific keys have mismatches, others don't

**Diagnosis:**
1. Run `compare_row_by_row()` for affected keys
2. Run `inspect_raw_cdc_files()` for one affected key
3. Check if those keys have UPDATE events

**Likely Cause:** Some UPDATE events missing or not applied correctly

**Fix:** Review deduplication logic and MERGE conditions

---

### Scenario 3: Staging table has NULL values

**Diagnosis:**
1. Run `check_merge_completeness()` to identify which columns have NULLs
2. Run `analyze_cdc_events_by_column_family()` to see if source CDC files have the data

**Likely Cause:** MERGE aggregation using `first()` instead of `first(ignorenulls=True)`

**Fix:** Update `merge_column_family_fragments()` to use `ignorenulls=True`

---

## Integration with Notebook

The debug module is designed to work seamlessly with `cockroachdb-cdc-tutorial.ipynb`:

- **Cell 14:** Detects sum mismatches and identifies problematic columns
- **Cell 15:** Uses debug functions to diagnose root cause
- **Cell 6:** Contains the MERGE logic that may need fixing

## Typical Workflow

1. **Run Cell 14** → Verify source/target sync
2. **If sums don't match** → Note which columns fail
3. **Run Cell 15** → Import and run debug functions
4. **Analyze output** → Identify root cause
5. **Fix Cell 6** → Update MERGE logic
6. **Re-run Cell 12** → Re-ingest CDC events
7. **Run Cell 14 again** → Verify fix

## Tips

- Start with `run_full_diagnosis()` for comprehensive analysis
- Use `inspect_raw_cdc_files()` for deep dive on specific keys
- Compare staging table vs final table to isolate MERGE vs dedup issues
- Check `__crdb__updated` timestamps to verify event ordering

## References

- Main notebook: `cockroachdb-cdc-tutorial.ipynb`
- Test strategy: `CONNECTOR_EVOLUTION_STRATEGY.md` (Multi-Column Sum Verification)
- Column family docs: CockroachDB documentation on column families
