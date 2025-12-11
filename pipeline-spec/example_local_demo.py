"""
Local Demo: Example Connector Data Flow

This script demonstrates how the example connector works locally
without requiring Databricks or Unity Catalog setup.

Run: python3 pipeline-spec/example_local_demo.py
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from sources.example.example import LakeflowConnect

def demo_connector():
    """Demonstrate the example connector functionality."""
    
    print("=" * 70)
    print("Example Connector Demo")
    print("=" * 70)
    print()
    
    # Step 1: Initialize the connector
    print("1. Initializing connector with options...")
    options = {
        "user": "demo_user",
        "password": "demo_password",
        "token": "demo_token"
    }
    connector = LakeflowConnect(options)
    print(f"   ✅ Connector initialized with options: {list(options.keys())}")
    print()
    
    # Step 2: List available tables
    print("2. Listing available tables...")
    tables = connector.list_tables()
    print(f"   ✅ Found {len(tables)} tables: {tables}")
    print()
    
    # Step 3: Get schema for each table
    print("3. Getting table schemas...")
    for table in tables:
        schema = connector.get_table_schema(table, {})
        print(f"   📋 {table}:")
        for field in schema.fields:
            print(f"      - {field.name}: {field.dataType.simpleString()}")
    print()
    
    # Step 4: Get table metadata
    print("4. Getting table metadata...")
    for table in tables:
        metadata = connector.read_table_metadata(table, {})
        print(f"   📊 {table}:")
        print(f"      - Primary keys: {metadata.get('primary_keys')}")
        print(f"      - Ingestion type: {metadata.get('ingestion_type')}")
    print()
    
    # Step 5: Read sample data
    print("5. Reading sample data...")
    for table in tables:
        print(f"\n   📁 Reading from {table}...")
        
        # Configure table options
        table_options = {"num_rows": 5}
        
        # Read the table
        data_iterator, offset = connector.read_table(
            table_name=table,
            start_offset={},
            table_options=table_options
        )
        
        # Consume and display the data
        records = list(data_iterator)
        print(f"      ✅ Retrieved {len(records)} records")
        print(f"      📍 Next offset: {offset}")
        
        # Show first 3 records
        for i, record in enumerate(records[:3]):
            print(f"      Record {i+1}: {record}")
        
        if len(records) > 3:
            print(f"      ... and {len(records) - 3} more records")
    
    print()
    print("=" * 70)
    print("✅ Demo completed successfully!")
    print("=" * 70)
    print()
    print("To use this in a Databricks pipeline:")
    print("  1. Create a Unity Catalog connection with these credentials")
    print("  2. Use pipeline-spec/example_connector_pipeline.py")
    print("  3. The pipeline will create streaming tables in your catalog")
    print()

if __name__ == "__main__":
    demo_connector()

