# Databricks notebook source
# MAGIC %md
# MAGIC # Test CockroachDB Connector - Volume Mode
# MAGIC 
# MAGIC Test the dual-mode cockroachdb.py connector reading from Unity Catalog Volume

# COMMAND ----------

# Configuration
CATALOG = "main"
SCHEMA = "robert_lee_cockroachdb"
VOLUME_PATH = "/Volumes/main/robert_lee_cockroachdb/parquet_files"
TABLE_NAME = "usertable"
OUTPUT_TABLE = f"{CATALOG}.{SCHEMA}.{TABLE_NAME}_volume_test"

print("=" * 80)
print("🧪 Testing CockroachDB Connector - Volume Mode")
print("=" * 80)
print(f"Volume Path: {VOLUME_PATH}")
print(f"Source Table: {TABLE_NAME}")
print(f"Output Table: {OUTPUT_TABLE}")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Verify Volume Has Files

# COMMAND ----------

print("📂 Verifying Volume...")
files = dbutils.fs.ls(VOLUME_PATH)
parquet_files = [f for f in files if f.name.endswith('.parquet')]

print(f"\n✅ Found {len(parquet_files)} Parquet files:")
for f in parquet_files[:5]:
    print(f"  • {f.name[:60]}... ({f.size:,} bytes)")
if len(parquet_files) > 5:
    print(f"  ... and {len(parquet_files) - 5} more")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Test CockroachDB Connector with Volume

# COMMAND ----------

from pyspark.sql import SparkSession

# Read using the connector
df = (
    spark.readStream
    .format("lakeflow_connect")
    .option("connector_module", "cockroachdb")
    .option("connector_class", "LakeflowConnect")
    .option("volume_path", VOLUME_PATH)  # Volume mode!
    .option("schema", "public")
    .load(TABLE_NAME)
)

print("✅ Connector initialized in Volume mode")
print("\n📊 Schema:")
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Write to Catalog Table

# COMMAND ----------

# Drop table if exists (for clean test)
spark.sql(f"DROP TABLE IF EXISTS {OUTPUT_TABLE}")
print(f"🗑️  Dropped existing table (if any): {OUTPUT_TABLE}")

# Write stream to Unity Catalog table
checkpoint_path = f"/tmp/cockroachdb_volume_test/{TABLE_NAME}/_checkpoint"

# Clean checkpoint (for clean test)
dbutils.fs.rm(checkpoint_path, recurse=True)

print(f"\n📝 Writing to Unity Catalog table...")
print(f"   Table: {OUTPUT_TABLE}")
print(f"   Checkpoint: {checkpoint_path}")

query = (
    df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)  # Process all available data once
    .toTable(OUTPUT_TABLE)  # Write to catalog table
)

# Wait for completion
query.awaitTermination()

print(f"\n✅ Stream processing complete!")
print(f"✅ Data written to: {OUTPUT_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Verify Results

# COMMAND ----------

# Read back the data from catalog table
result_df = spark.table(OUTPUT_TABLE)

print("📊 Results:")
print(f"   Table: {OUTPUT_TABLE}")
print(f"   Total rows: {result_df.count():,}")

print("\n📝 Schema:")
result_df.printSchema()

print("\n📋 Sample data:")
result_df.show(10, truncate=False)

# Show CDC metadata columns
print("\n🔍 CDC Metadata:")
result_df.select("_cdc_key", "_cdc_updated", "_cdc_operation", "_source_file").show(5, truncate=False)

print(f"\n✅ Volume Mode Test Complete!")
print(f"✅ Query results: SELECT * FROM {OUTPUT_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC ✅ **CockroachDB Connector - Volume Mode Tested**
# MAGIC 
# MAGIC **Configuration:**
# MAGIC - Mode: Volume (no database connection)
# MAGIC - Source: Unity Catalog Volume with Parquet files
# MAGIC - Output: Catalog table (Unity Catalog governed)
# MAGIC - Cursor: Filename-based (embedded timestamp)
# MAGIC 
# MAGIC **Results Table:**
# MAGIC ```sql
# MAGIC SELECT * FROM main.robert_lee_cockroachdb.usertable_volume_test;
# MAGIC ```
# MAGIC 
# MAGIC **Benefits:**
# MAGIC - ✅ No database credentials needed
# MAGIC - ✅ Reads pre-synced Parquet files  
# MAGIC - ✅ Unity Catalog governance (input & output)
# MAGIC - ✅ File-based cursor for exactly-once processing
# MAGIC - ✅ Queryable catalog tables (not temp paths)

