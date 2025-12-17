"""
CockroachDB Pipeline Configuration

Dynamically generates a pipeline spec based on DLT pipeline configuration.
Supports both single and multi-table configurations.

Usage:
1. Set connection_name in DLT pipeline configuration (required)
2. Optionally set table_list in DLT pipeline configuration (comma-separated)
3. All tables from the connection will be ingested (or only specified tables if table_list is set)

Pipeline Configuration Parameters:
- source_name: "cockroachdb" (required)
- connection_name: Name of the Unity Catalog connection (required)
- table_list: Comma-separated list of tables to ingest (optional, defaults to all tables)

Example configurations:
- All tables: {"source_name": "cockroachdb", "connection_name": "my_connection"}
- Specific tables: {"source_name": "cockroachdb", "connection_name": "my_connection", "table_list": "customers,orders"}
"""

from pipeline.ingestion_pipeline import ingest
from libs.source_loader import get_register_function
from sources.cockroachdb.cockroachdb import LakeflowConnect

source_name = "cockroachdb"

# Read configuration from DLT pipeline configuration (via Spark config)
# DLT exposes pipeline configuration as spark.conf, not os.environ
connection_name = spark.conf.get("connection_name", "cockroachdb_connection")
table_list_str = spark.conf.get("table_list", "")  # Optional: comma-separated list

print(f"DLT pipeline configuration - connection_name: {connection_name}, table_list: {table_list_str or 'all tables'}")

# Note: The connector will read connection options (host, port, etc.) from the Unity Catalog connection
# We don't need to pass them here - they're automatically retrieved by the Lakeflow framework
connector = LakeflowConnect({})

# Determine which tables to ingest
if table_list_str:
    # Use explicitly specified tables
    all_tables = [t.strip() for t in table_list_str.split(",") if t.strip()]
    print(f"Using specified tables: {all_tables}")
else:
    # Discover all available tables
    all_tables = connector.list_tables()
    print(f"Discovered {len(all_tables)} tables from connection: {all_tables}")

# Build pipeline spec with all tables
# For CockroachDB CDC, table_configuration can include:
# - initial_scan: 'yes' (default, full scan + streaming) or 'only' (snapshot only)
# - resolved_interval: Time between resolved timestamps (e.g., '1s', '10s')
# - batch_size: Number of rows to fetch per batch
default_table_config = {
    "initial_scan": "yes",      # Full scan + continue streaming
    "resolved_interval": "10s",  # Resolved timestamps every 10 seconds
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

print(f"Pipeline spec generated for {len(pipeline_spec['objects'])} tables")

register_lakeflow_source = get_register_function(source_name)
register_lakeflow_source(spark)
ingest(spark, pipeline_spec)

