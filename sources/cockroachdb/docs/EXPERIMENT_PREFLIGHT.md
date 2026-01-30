# Experiment Pre-Flight Checklist

## Error: "table does not exist"

If you see:
```
DatabaseError: table "defaultdb.public.usertable_append_only_multi_cf" does not exist
```

This means the test table hasn't been created yet.

---

## ✅ Pre-Flight Checklist

Before running the experiment, ensure these cells have been run:

### 1. Cell 3: Load Configuration ✓
```python
# Should print: ✅ Configuration loaded
```

### 2. Cell 5: Define Functions ✓
```python
# Defines get_cockroachdb_connection() and other helper functions
```

### 3. Cell 7: Create YCSB Table ✓
```python
# Creates and populates the table
# Should print: ✅ YCSB table created
```

**⚠️ CRITICAL**: The experiment needs the table to exist before creating a changefeed!

---

## Quick Check - Is Table Created?

Run this in a notebook cell:

```python
# Check if table exists
conn = get_cockroachdb_connection()
try:
    with conn.cursor() as cur:
        fully_qualified = f"{source_catalog}.{source_schema}.{source_table}"
        cur.execute(f"SELECT COUNT(*) FROM {fully_qualified}")
        count = cur.fetchone()[0]
        print(f"✅ Table exists: {fully_qualified}")
        print(f"   Row count: {count}")
except Exception as e:
    print(f"❌ Table does NOT exist!")
    print(f"   Error: {e}")
    print(f"\n💡 Run Cell 7 to create the table")
finally:
    conn.close()
```

---

## If Table Doesn't Exist

### Option 1: Run Cell 7 (Create Table)

This will:
1. Create YCSB table
2. Insert snapshot data (20 rows)
3. Print confirmation

### Option 2: Check if Table Was Dropped

Maybe you ran a cleanup cell? Check:

```python
# List tables in CockroachDB
conn = get_cockroachdb_connection()
try:
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = '{source_schema}'
            ORDER BY table_name
        """)
        tables = cur.fetchall()
        print(f"Tables in {source_schema}:")
        for table in tables:
            print(f"   - {table[0]}")
finally:
    conn.close()
```

---

## Complete Setup Sequence

If starting fresh:

```python
# 1. Run Cell 3: Configuration
# (Run the config cell)

# 2. Run Cell 5: Functions  
# (Run the functions cell)

# 3. Run Cell 7: Create Table
# (Run the YCSB table creation cell)

# 4. NOW run the experiment
from cockroachdb_experiments import test_azure_path_prefix

results = test_azure_path_prefix(
    storage_account_name, storage_account_key, container_name,
    source_catalog, source_schema, source_table, target_table,
    column_family_mode, get_cockroachdb_connection, storage_account_key_encoded
)
```

---

## Experiment Now Has Built-in Check

The updated `test_azure_path_prefix()` function now automatically:

1. ✅ Checks if table exists **before** creating changefeed
2. ❌ Exits early with clear error message if table is missing
3. 💡 Tells you to "Run Cell 7" to fix it

So if you forgot to create the table, you'll see:

```
================================================================================
Verifying Table Exists
================================================================================
❌ ERROR: Table does not exist!
   Table: defaultdb.public.usertable_append_only_multi_cf
   
💡 Solution: Run Cell 7 to create and populate the table first
```

---

## Date

2026-01-30
