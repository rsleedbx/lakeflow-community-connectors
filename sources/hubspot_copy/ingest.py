"""
HubSpot Copy Ingestion Pipeline

Simple pipeline configuration for testing HubSpot connector with
CockroachDB deployment scripts.
"""

from pipeline.ingestion_pipeline import ingest
from libs.source_loader import get_register_function

source_name = "hubspot_copy"

# Simple pipeline spec - update as needed for your testing
pipeline_spec = {
    "connection_name": "hubspot_demo",  # Update to your connection name
    "objects": [
        {
            "table": {
                "source_table": "contacts",  # HubSpot table to test
            }
        },
    ],
}

# Register and run pipeline
register_lakeflow_source = get_register_function(source_name)
register_lakeflow_source(spark)
ingest(spark, pipeline_spec)
