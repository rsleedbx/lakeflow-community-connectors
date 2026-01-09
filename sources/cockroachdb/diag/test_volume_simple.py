# Databricks notebook source
# MAGIC %md
# MAGIC # Simple Volume Mode Test
# MAGIC 
# MAGIC Quick test of cockroachdb.py Volume mode - run this manually in Databricks UI

# COMMAND ----------

# Install connector module path
import sys
sys.path.insert(0, '/Workspace/Users/robert.lee@databricks.com/cockroachdb_tests')

# COMMAND ----------

# Import and test
from cockroachdb import LakeflowConnect

# Configuration
options = {
    "volume_path": "/Volumes/main/robert_lee_cockroachdb/parquet_files",
    "schema": "public"
}

# Initialize connector
connector = LakeflowConnect(options)

print(f"\n✅ Connector initialized successfully!")
print(f"Mode: {connector.mode}")
print(f"Volume Path: {connector.volume_path}")

# COMMAND ----------

# Test read_table method
table_name = "usertable"
start_offset = {}
table_options = {}

print(f"\n📖 Reading table: {table_name}")
print(f"Start offset: {start_offset}")

try:
    rows_iter, end_offset = connector.read_table(table_name, start_offset, table_options)
    
    # Convert iterator to list (limit to first 100 rows for testing)
    rows = []
    for idx, row in enumerate(rows_iter):
        if idx >= 100:  # Limit for testing
            break
        rows.append(row)
    
    print(f"\n✅ Successfully read {len(rows)} rows")
    print(f"End offset: {end_offset}")
    
    # Show first few rows
    print(f"\n📋 Sample rows:")
    for row in rows[:3]:
        print(f"  {row}")
        
except Exception as e:
    print(f"\n❌ Error: {e}")
    import traceback
    traceback.print_exc()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Expected Output:
# MAGIC 
# MAGIC - ✅ Mode: volume
# MAGIC - ✅ Reads Parquet files from Volume
# MAGIC - ✅ Returns rows with CDC metadata
# MAGIC - ✅ Cursor updated to last filename

