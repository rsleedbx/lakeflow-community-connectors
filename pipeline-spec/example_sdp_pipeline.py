"""
Example Spark Declarative Pipeline (SDP) using the Example Connector

This is a valid SDP that can be used with Databricks DLT/Workflows.

Prerequisites:
1. The _generated_example_python_source.py file must exist
   (run: python3 scripts/merge_python_source.py example)
2. A Unity Catalog connection 'example_connection' must be created
3. Deploy as a DLT pipeline or Databricks workflow

Configuration:
- Update connection_name to match your UC connection
- Update catalog/schema names to match your workspace
- Adjust table_configuration as needed
"""

from pyspark import pipelines as sdp
from pyspark.sql.functions import col
from libs.source_loader import get_register_function

# =============================================================================
# CONFIGURATION
# =============================================================================

source_name = "example"
connection_name = "example_connection"

# Register the connector (must be done at module level for SDP)
register_lakeflow_source = get_register_function(source_name)
register_lakeflow_source(spark)

# =============================================================================
# TABLE 1: my_table (APPEND mode)
# =============================================================================

# Create streaming table
sdp.create_streaming_table(name="main.default.my_table")

# Define append flow
@sdp.append_flow(name="my_table_flow", target="main.default.my_table")
def ingest_my_table():
    return (
        spark.readStream.format("lakeflow_connect")
        .option("databricks.connection", connection_name)
        .option("tableName", "my_table")
        .option("num_rows", "100")  # Example-specific option
        .load()
    )

# =============================================================================
# TABLE 2: your_table (APPEND mode)
# =============================================================================

# Create streaming table
sdp.create_streaming_table(name="main.default.your_table")

# Define append flow
@sdp.append_flow(name="your_table_flow", target="main.default.your_table")
def ingest_your_table():
    return (
        spark.readStream.format("lakeflow_connect")
        .option("databricks.connection", connection_name)
        .option("tableName", "your_table")
        .option("num_rows", "50")  # Example-specific option
        .load()
    )

# =============================================================================
# NOTES
# =============================================================================
# This SDP will create two streaming tables in your catalog:
# - main.default.my_table (100 generated rows)
# - main.default.your_table (50 generated rows)
#
# To run this pipeline:
# 1. Upload to Databricks workspace
# 2. Create a DLT pipeline pointing to this file
# 3. Or run as a Databricks job with serverless compute

