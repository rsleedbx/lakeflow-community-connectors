"""
Example Pipeline using the Example Connector

This demonstrates how to create a Spark Declarative Pipeline (SDP) 
using the example connector as the data source.

Prerequisites:
1. The _generated_example_python_source.py file must exist
   (run: python3 scripts/merge_python_source.py example)
2. A Unity Catalog connection must be created with the example connector
3. Run this in a Databricks notebook or job

Usage:
    # In a Databricks notebook
    %run /path/to/example_connector_pipeline
"""

from pipeline.ingestion_pipeline import ingest
from libs.source_loader import get_register_function

# =============================================================================
# CONFIGURATION
# =============================================================================

source_name = "example"

# Unity Catalog connection name (you need to create this first)
# Connection options should include:
#   - user: "example_user"
#   - password: "example_password"  
#   - token: "example_token"
connection_name = "example_connection"

# Pipeline specification
pipeline_spec = {
    "connection_name": connection_name,
    "objects": [
        # Ingest "my_table" 
        {
            "table": {
                "source_table": "my_table",
                "destination_catalog": "main",  # Change to your catalog
                "destination_schema": "default",  # Change to your schema
                "destination_table": "my_table",
                "table_configuration": {
                    "num_rows": 100,  # Example-specific: number of rows to generate
                },
            }
        },
        # Ingest "your_table"
        {
            "table": {
                "source_table": "your_table",
                "destination_catalog": "main",
                "destination_schema": "default",
                "destination_table": "your_table",
                "table_configuration": {
                    "num_rows": 50,
                },
            }
        },
    ],
}

# =============================================================================
# PIPELINE EXECUTION
# =============================================================================

if __name__ == "__main__":
    # Step 1: Register the example source with Spark
    print(f"Registering {source_name} connector...")
    register_lakeflow_source = get_register_function(source_name)
    register_lakeflow_source(spark)
    
    # Step 2: Run the ingestion pipeline
    print(f"Starting ingestion pipeline...")
    print(f"  Connection: {connection_name}")
    print(f"  Tables: {[obj['table']['source_table'] for obj in pipeline_spec['objects']]}")
    
    ingest(spark, pipeline_spec)
    
    print("✅ Pipeline execution completed!")

