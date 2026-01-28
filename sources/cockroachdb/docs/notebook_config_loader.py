# ============================================================================
# CELL 2: CONFIGURATION (Load from file with embedded fallback)
# ============================================================================
import json
import os
from pathlib import Path
from urllib.parse import quote

# Try to load config from external file, fallback to embedded config
config = None
config_file_paths = [
    # Option 1: Local .env directory (for development)
    "/Users/robert.lee/github/lakeflow-community-connectors/sources/cockroachdb/.env/cockroachdb_cdc_tutorial_config.json",
    # Option 2: Same directory as notebook (Databricks workspace)
    "cockroachdb_cdc_tutorial_config.json",
    # Option 3: Unity Catalog Volume (uncomment and modify as needed)
    # "/Volumes/main/your_schema/your_volume/cockroachdb_cdc_tutorial_config.json",
]

for config_path in config_file_paths:
    if os.path.exists(config_path):
        try:
            with open(config_path, 'r') as f:
                config = json.load(f)
            print(f"✅ Configuration loaded from: {config_path}")
            break
        except Exception as e:
            print(f"⚠️  Failed to load {config_path}: {e}")
            continue

# Fallback to embedded configuration if no external file found
if config is None:
    print("ℹ️  Using embedded configuration (no external config file found)")
    config = {
      "cockroachdb": {
        "host": "REPLACE_WITH_YOUR_HOST",
        "port": 26257,
        "user": "REPLACE_WITH_YOUR_USER",
        "password": "REPLACE_WITH_YOUR_PASSWORD",
        "database": "defaultdb"
      },
      "cockroachdb_source": {
        "catalog": "defaultdb",
        "schema": "public",
        "table_name": "usertable",
        "_comment": "Base table name. If auto_suffix_mode_family=true, _{mode}_{column_family_mode} will be appended"
      },
      "azure_storage": {
        "account_name": "REPLACE_WITH_YOUR_STORAGE_ACCOUNT",
        "account_key": "REPLACE_WITH_YOUR_STORAGE_KEY",
        "container_name": "changefeed-events"
      },
      "databricks_target": {
        "catalog": "main",
        "schema": "REPLACE_WITH_YOUR_SCHEMA",
        "table_name": "usertable",
        "_comment": "Base table name. If auto_suffix_mode_family=true, _{mode}_{column_family_mode} will be appended"
      },
      "cdc_config": {
        "mode": "append_only",
        "_mode_options": "append_only (SCD Type 2, keeps history) or update_delete (SCD Type 1, latest state)",
        "column_family_mode": "single_cf",
        "_column_family_options": "single_cf (no column families) or multi_cf (with split_column_families)",
        "primary_key_columns": ["ycsb_key"],
        "auto_suffix_mode_family": true,
        "_auto_suffix_comment": "If true, automatically append _{mode}_{column_family_mode} to table names if not already present"
      },
      "workload_config": {
        "snapshot_count": 10,
        "insert_count": 10,
        "update_count": 9,
        "delete_count": 8,
        "_comment": "Net growth per cycle: insert_count - delete_count = 2 rows"
      }
    }

# Extract configuration values
cockroachdb_host = config["cockroachdb"]["host"]
cockroachdb_port = config["cockroachdb"]["port"]
cockroachdb_user = config["cockroachdb"]["user"]
cockroachdb_password = config["cockroachdb"]["password"]
cockroachdb_database = config["cockroachdb"]["database"]

source_catalog = config["cockroachdb_source"]["catalog"]
source_schema = config["cockroachdb_source"]["schema"]
source_table = config["cockroachdb_source"]["table_name"]

storage_account_name = config["azure_storage"]["account_name"]
storage_account_key = config["azure_storage"]["account_key"]
storage_account_key_encoded = quote(storage_account_key, safe='')
container_name = config["azure_storage"]["container_name"]

target_catalog = config["databricks_target"]["catalog"]
target_schema = config["databricks_target"]["schema"]
target_table = config["databricks_target"]["table_name"]

cdc_mode = config["cdc_config"]["mode"]
column_family_mode = config["cdc_config"]["column_family_mode"]
primary_key_columns = config["cdc_config"]["primary_key_columns"]

snapshot_count = config["workload_config"]["snapshot_count"]
insert_count = config["workload_config"]["insert_count"]
update_count = config["workload_config"]["update_count"]
delete_count = config["workload_config"]["delete_count"]

# Auto-suffix table names with mode and column family if enabled
auto_suffix = config["cdc_config"].get("auto_suffix_mode_family", False)
if auto_suffix:
    suffix = f"_{cdc_mode}_{column_family_mode}"
    
    # Add suffix to source_table if not already present
    if not source_table.endswith(suffix):
        source_table = f"{source_table}{suffix}"
    
    # Add suffix to target_table if not already present
    if not target_table.endswith(suffix):
        target_table = f"{target_table}{suffix}"

# Display configuration summary
print(f"\n📊 Configuration Summary:")
print(f"   CockroachDB: {cockroachdb_host}:{cockroachdb_port}/{cockroachdb_database}")
print(f"   Source: {source_catalog}.{source_schema}.{source_table}")
print(f"   Target: {target_catalog}.{target_schema}.{target_table}")
print(f"   Azure Storage: {storage_account_name}/{container_name}")
print(f"\n🔧 CDC Settings:")
print(f"   Processing Mode: {cdc_mode}")
print(f"   Column Family Mode: {column_family_mode}")
print(f"   Primary Keys: {primary_key_columns}")
print(f"   Auto-suffix: {auto_suffix}")
print(f"\n📈 Workload:")
print(f"   {snapshot_count} snapshot → +{insert_count} INSERTs, ~{update_count} UPDATEs, -{delete_count} DELETEs")
print(f"   Net growth per cycle: {insert_count - delete_count} rows")
