"""
CockroachDB Pipeline Configuration

Generates a pipeline spec based on DLT pipeline configuration.
Requires explicit table specification.

Usage:
1. Set connection_name in DLT pipeline configuration (required)
2. Set table_list in DLT pipeline configuration (required, comma-separated)

Pipeline Configuration Parameters:
- source_name: "cockroachdb" (required)
- connection_name: Name of the Unity Catalog connection (required)
- table_list: Comma-separated list of tables to ingest (required)

Example configurations:
- Single table:  {"source_name": "cockroachdb", "connection_name": "my_connection", "table_list": "usertable"}
- Multiple tables: {"source_name": "cockroachdb", "connection_name": "my_connection", "table_list": "customers,orders,products"}

Note: Community Connectors require explicit table specification because connection 
credentials are only available during Spark execution, not during pipeline configuration.
Unlike Managed Connectors (SQL Server, etc.), Community Connectors also do not support
schema-level ingestion.

For YCSB workload testing, use "usertable" as the table_list.

To discover available tables:
1. Connect to your CockroachDB cluster
2. Run: SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';
3. Specify the tables you want in table_list parameter
"""

from pipeline.ingestion_pipeline import ingest
from libs.source_loader import get_register_function

source_name = "cockroachdb"

print("=" * 80)
print("🔍 DEBUG: CockroachDB Ingest Pipeline Starting")
print("=" * 80)

# Debug: Print all DLT configuration for troubleshooting
print("\n📋 All DLT Configuration Keys:")
try:
    # spark.conf.getAll() can return either a list of tuples or a dict depending on Spark version
    all_conf = spark.conf.getAll()
    
    # Convert to dict if it's a list of tuples
    if isinstance(all_conf, list):
        conf_dict = dict(all_conf)
    else:
        conf_dict = all_conf
    
    # Filter for DLT configuration keys
    dlt_conf = {k: v for k, v in conf_dict.items() if "databricks.pipeline.configuration" in k}
    
    if dlt_conf:
        for key, val in dlt_conf.items():
            print(f"  {key}: {val}")
    else:
        print("  ⚠️  No DLT configuration keys found!")
        print(f"  Total spark.conf keys: {len(conf_dict)}")
        # Print a few keys for debugging
        print("  Sample keys:")
        for i, key in enumerate(list(conf_dict.keys())[:5]):
            print(f"    {key}")
except Exception as e:
    print(f"  ❌ Error reading spark configuration: {e}")

# Read configuration from DLT pipeline configuration (via Spark config)
# DLT exposes pipeline configuration with the prefix: spark.databricks.pipeline.configuration.
print("\n📖 Reading Pipeline Configuration:")
connection_name = spark.conf.get("spark.databricks.pipeline.configuration.connection_name", "cockroachdb_connection")
table_list_str = spark.conf.get("spark.databricks.pipeline.configuration.table_list", "")  # Required: comma-separated list

print(f"  ✓ connection_name: '{connection_name}'")
print(f"  ✓ table_list: '{table_list_str or 'NOT SET'}'")
print(f"  ✓ source_name: '{source_name}'")

# Validate and parse table list
if not table_list_str:
    raise ValueError(
        "table_list is required but not set in pipeline configuration.\n"
        "\n"
        "Please specify tables to ingest as a comma-separated list.\n"
        "Example: table_list='customers,orders,products'\n"
        "\n"
        "To discover available tables, connect to your CockroachDB cluster and run:\n"
        "  SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';\n"
        "\n"
        "Why is this required?\n"
        "  Community Connectors cannot dynamically discover tables because connection\n"
        "  credentials are only available during Spark execution, after the pipeline\n"
        "  spec has already been defined."
    )

# Parse table list
all_tables = [t.strip() for t in table_list_str.split(",") if t.strip()]

if not all_tables:
    raise ValueError(
        "table_list is empty after parsing. Please specify at least one table.\n"
        "Example: table_list='customers,orders'"
    )

print(f"Tables to ingest ({len(all_tables)}): {all_tables}")

# Register the source
register_lakeflow_source = get_register_function(source_name)
register_lakeflow_source(spark)

# Build pipeline spec with specified tables
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

print(f"\n✅ Pipeline spec generated for {len(pipeline_spec['objects'])} tables")
print(f"   Tables to ingest: {', '.join(all_tables)}")
print(f"\n🔗 Connection Details:")
print(f"   Connection name: {connection_name}")
print(f"   This connection will be resolved by Databricks from Unity Catalog")
print(f"   Expected to have: host, port, database, user, password, sslmode")

print("\n" + "=" * 80)
print("🚀 Starting Ingestion Pipeline...")
print("=" * 80 + "\n")

# Run the ingestion pipeline
ingest(spark, pipeline_spec)

