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

import os
from pipeline.ingestion_pipeline import ingest
from libs.source_loader import get_register_function

source_name = "cockroachdb"

print("=" * 80)
print("🔍 DEBUG: CockroachDB Ingest Pipeline Starting")
print("=" * 80)

# Debug: Check environment variables (DLT might use these)
print("\n🌍 Environment Variables (filtered for config):")
config_env_vars = {k: v for k, v in os.environ.items() if any(x in k.lower() for x in ['connection', 'table', 'source', 'databricks'])}
if config_env_vars:
    for key, val in sorted(config_env_vars.items()):
        print(f"  {key}: {val}")
else:
    print("  ⚠️  No config-related environment variables found")

# Debug: Check Spark configuration
print("\n⚙️  Spark Configuration (all keys):")
try:
    # Try as property first (newer Spark)
    try:
        all_conf = spark.conf.getAll
        if callable(all_conf):
            all_conf = all_conf()
    except:
        all_conf = spark.conf.getAll()
    
    # Convert to dict if needed
    if isinstance(all_conf, list):
        conf_dict = dict(all_conf)
    elif isinstance(all_conf, dict):
        conf_dict = all_conf
    else:
        conf_dict = {}
    
    print(f"  Total keys: {len(conf_dict)}")
    
    # Show config-related keys
    config_keys = {k: v for k, v in conf_dict.items() if 'configuration' in k.lower() or 'connection' in k.lower()}
    if config_keys:
        print("  Configuration-related keys:")
        for key, val in sorted(config_keys.items()):
            print(f"    {key}: {val}")
    else:
        print("  ⚠️  No configuration keys found in spark.conf")
        print("  Sample of first 10 keys:")
        for key in list(conf_dict.keys())[:10]:
            print(f"    {key}")
            
except Exception as e:
    print(f"  ❌ Error reading spark configuration: {e}")

# Read configuration from DLT pipeline configuration (via Spark config)
# DLT exposes pipeline configuration directly as spark.conf keys (NO prefix needed)
# This matches the pattern used by nullsource connector
print("\n📖 Reading Pipeline Configuration:")

connection_name = spark.conf.get("connection_name", None)
table_list_str = spark.conf.get("table_list", None)

if not connection_name:
    raise ValueError(
        "connection_name is required but not set in pipeline configuration.\n"
        "\n"
        "Please set connection_name in your DLT pipeline configuration.\n"
        "Example: {\"connection_name\": \"robert_lee_battle-walrus-11108\", \"table_list\": \"usertable\"}\n"
        "\n"
        "Check your pipeline settings to ensure configuration is properly set."
    )

if not table_list_str:
    raise ValueError(
        "table_list is required but not set in pipeline configuration.\n"
        "\n"
        "Please set table_list in your DLT pipeline configuration.\n"
        "Example: {\"connection_name\": \"my_connection\", \"table_list\": \"usertable\"}\n"
        "\n"
        "To discover available tables, connect to your CockroachDB cluster and run:\n"
        "  SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"
    )

print(f"  ✓ connection_name: '{connection_name}'")
print(f"  ✓ table_list: '{table_list_str}'")
print(f"  ✓ source_name: '{source_name}'")

# Parse table list
print(f"\n🔄 Parsing table list...")
all_tables = [t.strip() for t in table_list_str.split(",") if t.strip()]

if not all_tables:
    raise ValueError(
        "table_list is empty after parsing (possibly only whitespace/commas).\n"
        "\n"
        "Please specify at least one valid table name.\n"
        "Example: table_list='usertable' or table_list='customers,orders'"
    )

print(f"  ✓ Parsed {len(all_tables)} table(s): {all_tables}")

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

