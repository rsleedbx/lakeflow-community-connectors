"""
CockroachDB Pipeline Configuration

Generates a pipeline spec based on DLT pipeline configuration.
Supports both explicit table lists and dynamic table discovery.

Usage:
1. Set connection_name in DLT pipeline configuration (required)
2. Optionally set table_list in DLT pipeline configuration (comma-separated)
   - If table_list is provided: Only those tables will be ingested
   - If table_list is empty/not set: All tables from list_tables() will be ingested

Pipeline Configuration Parameters:
- source_name: "cockroachdb" (required)
- connection_name: Name of the Unity Catalog connection (required)
- table_list: Comma-separated list of tables to ingest (optional, defaults to all tables)

Example configurations:
- All tables: {"source_name": "cockroachdb", "connection_name": "my_connection"}
- Specific tables: {"source_name": "cockroachdb", "connection_name": "my_connection", "table_list": "customers,orders"}

Note: Unlike Managed Connectors (SQL Server, etc.), Community Connectors do not support
schema-level ingestion. However, we support dynamic table discovery via list_tables().
"""

from pipeline.ingestion_pipeline import ingest
from libs.source_loader import get_register_function
from sources.cockroachdb.cockroachdb import LakeflowConnect

source_name = "cockroachdb"

# Read configuration from DLT pipeline configuration (via Spark config)
# DLT exposes pipeline configuration as spark.conf, not os.environ
connection_name = spark.conf.get("connection_name", "cockroachdb_connection")
table_list_str = spark.conf.get("table_list", "")  # Optional: comma-separated list

print(f"DLT pipeline configuration - connection_name: {connection_name}, table_list: {table_list_str or 'all tables (via list_tables())'}")

# Determine which tables to ingest
if table_list_str:
    # Use explicitly specified tables
    all_tables = [t.strip() for t in table_list_str.split(",") if t.strip()]
    if not all_tables:
        raise ValueError(
            "table_list is empty after parsing. Please specify at least one table. "
            "Example: table_list='customers,orders'"
        )
    print(f"Using specified tables ({len(all_tables)}): {all_tables}")
else:
    # Dynamic table discovery: We'll get tables after connection is established
    # by doing a metadata read which will trigger list_tables()
    all_tables = None  # Signal to discover dynamically
    print(f"No table_list specified - will discover tables dynamically via list_tables()")

# If we need dynamic table discovery, we must first trigger a connection
# to call list_tables(). We do this by creating a minimal pipeline spec
# with a metadata-only read.
if all_tables is None:
    print("Performing dynamic table discovery...")
    
    # Create a minimal spec to trigger metadata read, which will call list_tables()
    # The connector's list_tables() will be called during metadata retrieval
    discovery_spec = {
        "connection_name": connection_name,
        "objects": [
            {
                "table": {
                    "source_table": "_discovery_placeholder"
                }
            }
        ]
    }
    
    # Register the source (only needs to be done once)
    register_lakeflow_source = get_register_function(source_name)
    register_lakeflow_source(spark)
    
    # Trigger a metadata read to instantiate the connector and call list_tables()
    # We use a special metadata read that will invoke the connector's list_tables()
    try:
        # Read from a special "list_tables" table that will trigger list_tables()
        df = (
            spark.read.format("lakeflow_connect")
            .option("databricks.connection", connection_name)
            .option("tableName", "_lakeflow_metadata")
            .option("tableNameList", "")  # Empty list triggers list_tables()
            .load()
        )
        
        # Collect the discovered tables
        discovered_tables = []
        for row in df.collect():
            table_name = row["tableName"]
            if table_name:  # Filter out any null or empty names
                discovered_tables.append(table_name)
        
        all_tables = discovered_tables
        print(f"✅ Discovered {len(all_tables)} tables: {all_tables}")
        
        if not all_tables:
            raise ValueError(
                "No tables discovered via list_tables(). "
                "Either the connection has no tables, or list_tables() returned an empty list. "
                "Please check your connection and database."
            )
    except Exception as e:
        raise ValueError(
            f"Failed to discover tables dynamically: {e}\n"
            "Please specify table_list explicitly in the pipeline configuration."
        ) from e
else:
    # Register the source (only needs to be done once)
    register_lakeflow_source = get_register_function(source_name)
    register_lakeflow_source(spark)

# Build pipeline spec with all tables (discovered or explicit)
# For CockroachDB CDC, table_configuration can include:
# - initial_scan: 'yes' (default, full scan + streaming) or 'only' (snapshot only)
# - resolved_interval: Time between resolved timestamps (e.g., '1s', '10s')
# - batch_size: Number of rows to fetch per batch
# - split_column_families: 'true' to emit separate events for column families
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
print(f"Tables to ingest: {', '.join(all_tables)}")

# Run the ingestion pipeline
ingest(spark, pipeline_spec)

