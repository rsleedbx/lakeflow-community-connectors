from pipeline.ingestion_pipeline import ingest
from libs.source_loader import get_register_function

source_name = "nullsource"

pipeline_spec = {
    "connection_name": "nullsource_connection",
    "objects": [
        {
            "table": {
                "source_table": "intpk",
                "table_configuration": {
                    "num_rows": "1000",
                }
            }
        }
    ],
}

register_lakeflow_source = get_register_function(source_name)
register_lakeflow_source(spark)
ingest(spark, pipeline_spec)

