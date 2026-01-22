# Add this code RIGHT AFTER the merge_column_family_fragments call
# BEFORE the "Filter out DELETE operations" section

# Deduplicate CDC events (keep latest by timestamp for each PK + operation)
# This matches the behavior of analyze_volume_changefeed_files
print(f"\n🔧 Deduplicating CDC events...")
from pyspark.sql import Window
from pyspark.sql.functions import row_number, col

# Create window partitioned by PK + operation, ordered by timestamp descending
window_spec = Window.partitionBy(primary_keys + ['_cdc_operation']).orderBy(col('_cdc_updated').desc())

# Keep only the latest event for each (PK + operation) combination
df_deduped = df_merged.withColumn("_row_num", row_number().over(window_spec)) \
                      .filter(col("_row_num") == 1) \
                      .drop("_row_num")

deduped_count = df_merged.count() - df_deduped.count()
print(f"   Removed {deduped_count} duplicate CDC events")
print(f"   After deduplication: {df_deduped.count():,} rows")

# Update the df_merged variable to use the deduplicated version
df_merged = df_deduped
