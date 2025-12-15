"""
Nullsource Pipeline Configuration

Dynamically generates a pipeline spec based on DLT pipeline configuration.
Supports both single and multi-table configurations.

Usage:
1. Set num_tables in DLT pipeline configuration (default: 1)
2. Set num_rows in DLT pipeline configuration (default: 1000)
3. All tables discovered via list_tables() will be ingested automatically
"""

from pipeline.ingestion_pipeline import ingest
from libs.source_loader import get_register_function
from sources.nullsource.nullsource import LakeflowConnect

source_name = "nullsource"
connection_name = "nullsource_connection"

# Read configuration from DLT pipeline configuration (via Spark config)
# DLT exposes pipeline configuration as spark.conf, not os.environ
num_tables_str = spark.conf.get("num_tables", "1")
num_rows_default = spark.conf.get("num_rows", "1000")

print(f"DLT pipeline configuration - num_tables: {num_tables_str}, num_rows: {num_rows_default}")

connection_options = {"num_tables": num_tables_str}
print(f"Connector options: {connection_options}")

connector = LakeflowConnect(connection_options)
all_tables = connector.list_tables()

print(f"Discovered {len(all_tables)} tables from connection: {all_tables}")

# Default table configuration for all tables (uses num_rows from pipeline config)
default_table_config = {
    "num_rows": num_rows_default
}

pipeline_spec = {
    "connection_name": connection_name,
    "objects": [
        {
            "table": {
                "source_table": table_name,
                "table_configuration": default_table_config
            }
        }
        for table_name in all_tables
    ]
}

register_lakeflow_source = get_register_function(source_name)
register_lakeflow_source(spark)
ingest(spark, pipeline_spec)
