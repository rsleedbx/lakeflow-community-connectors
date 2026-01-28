"""
Quick code to load configuration from JSON file for the CockroachDB CDC Tutorial notebook.

Usage in Databricks notebook:
    # Cell 1: Load configuration
    import json
    from urllib.parse import quote
    
    # Load config from JSON file
    with open("cockroachdb_cdc_tutorial_config.json", "r") as f:
        config = json.load(f)
    
    # Extract CockroachDB connection
    cockroachdb_host = config["cockroachdb"]["host"]
    cockroachdb_port = config["cockroachdb"]["port"]
    cockroachdb_user = config["cockroachdb"]["user"]
    cockroachdb_password = config["cockroachdb"]["password"]
    cockroachdb_database = config["cockroachdb"]["database"]
    
    # Extract CockroachDB source
    source_catalog = config["cockroachdb_source"]["catalog"]
    source_schema = config["cockroachdb_source"]["schema"]
    source_table = config["cockroachdb_source"]["table_name"]
    
    # Extract Azure Storage
    storage_account_name = config["azure_storage"]["account_name"]
    storage_account_key = config["azure_storage"]["account_key"]
    storage_account_key_encoded = quote(storage_account_key, safe='')
    container_name = config["azure_storage"]["container_name"]
    
    # Extract Databricks target
    target_catalog = config["databricks_target"]["catalog"]
    target_schema = config["databricks_target"]["schema"]
    target_table = config["databricks_target"]["table_name"]
    
    # Extract CDC config
    cdc_mode = config["cdc_config"]["mode"]
    column_family_mode = config["cdc_config"]["column_family_mode"]
    primary_key_columns = config["cdc_config"]["primary_key_columns"]
    
    # Extract workload config
    snapshot_count = config["workload_config"]["snapshot_count"]
    insert_count = config["workload_config"]["insert_count"]
    update_count = config["workload_config"]["update_count"]
    delete_count = config["workload_config"]["delete_count"]
    
    print("✅ Configuration loaded from JSON")
    print(f"   CDC Processing Mode: {cdc_mode}")
    print(f"   Column Family Mode: {column_family_mode}")
    print(f"   Primary Keys: {primary_key_columns}")
    print(f"   Target Table: {target_table}")
    print(f"   CDC Workload: {snapshot_count} snapshot → +{insert_count} INSERTs, ~{update_count} UPDATEs, -{delete_count} DELETEs")
"""

# =============================================================================
# COMPACT VERSION (copy-paste into notebook Cell 2)
# =============================================================================

COMPACT_VERSION = """
# ============================================================================
# CELL 2: LOAD CONFIGURATION FROM JSON
# ============================================================================
import json
from urllib.parse import quote

# Load config from JSON file
with open("cockroachdb_cdc_tutorial_config.json", "r") as f:
    config = json.load(f)

# CockroachDB connection
cockroachdb_host = config["cockroachdb"]["host"]
cockroachdb_port = config["cockroachdb"]["port"]
cockroachdb_user = config["cockroachdb"]["user"]
cockroachdb_password = config["cockroachdb"]["password"]
cockroachdb_database = config["cockroachdb"]["database"]

# CockroachDB source
source_catalog = config["cockroachdb_source"]["catalog"]
source_schema = config["cockroachdb_source"]["schema"]
source_table = config["cockroachdb_source"]["table_name"]

# Azure Storage
storage_account_name = config["azure_storage"]["account_name"]
storage_account_key = config["azure_storage"]["account_key"]
storage_account_key_encoded = quote(storage_account_key, safe='')
container_name = config["azure_storage"]["container_name"]

# Databricks target
target_catalog = config["databricks_target"]["catalog"]
target_schema = config["databricks_target"]["schema"]
target_table = config["databricks_target"]["table_name"]

# CDC config
cdc_mode = config["cdc_config"]["mode"]
column_family_mode = config["cdc_config"]["column_family_mode"]
primary_key_columns = config["cdc_config"]["primary_key_columns"]

# Workload config
snapshot_count = config["workload_config"]["snapshot_count"]
insert_count = config["workload_config"]["insert_count"]
update_count = config["workload_config"]["update_count"]
delete_count = config["workload_config"]["delete_count"]

print("✅ Configuration loaded from JSON")
print(f"   CDC Processing Mode: {cdc_mode}")
print(f"   Column Family Mode: {column_family_mode}")
print(f"   Primary Keys: {primary_key_columns}")
print(f"   Target Table: {target_table}")
print(f"   CDC Workload: {snapshot_count} snapshot → +{insert_count} INSERTs, ~{update_count} UPDATEs, -{delete_count} DELETEs")
"""

if __name__ == "__main__":
    print("=" * 80)
    print("COMPACT VERSION - Copy this into your notebook Cell 2:")
    print("=" * 80)
    print(COMPACT_VERSION)
