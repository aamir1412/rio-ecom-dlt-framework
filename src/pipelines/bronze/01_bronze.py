# src/pipelines/01_bronze.py

import sys
import os

try:
    # Attempt to grab the path from the Databricks context
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    # /Workspace/Shared/.bundle/rio-ecom-dlt-framework/dev/files/src/pipelines/silver/silver_products.py
    # We split on 'src' to dynamically find the root regardless of how deep we are
    project_root = f"/Workspace{notebook_path.split('/src/')[0]}"
except Exception:
    # Fallback if dbutils is unavailable (e.g., local testing)
    current_dir = os.getcwd()
    project_root = current_dir.split('/src/')[0] if '/src/' in current_dir else current_dir

if project_root not in sys.path:
    sys.path.append(project_root) 
 
import importlib
import dlt
from pyspark.sql import SparkSession
from src.shared.audit import add_bronze_metadata
from src.config.bronze_config import INGESTION_CONFIG as ingestion_config

spark = SparkSession.builder.getOrCreate()
landing_zone_base = "/Volumes/cat_ecom_dev/raw/vol_landing_zone"
checkpoint_base = "/Volumes/cat_ecom_dev/raw/schema_checkpoints"

def generate_bronze_table(table_entity: str, config: dict):
    
    module = importlib.import_module(config["schema_module"])
    schema_obj = getattr(module, config["schema_obj"])
    target_path = config["path"]
    
    @dlt.table(
        name=f"bronze_{table_entity}",
        comment=f"Raw ingested {table_entity} via Auto Loader",
        table_properties={"quality": "bronze"}
    )
    def create_table():
        df_raw = (spark.readStream
                  .format("cloudFiles")
                  .option("cloudFiles.format", "csv")
                  .option("cloudFiles.schemaLocation", f"{checkpoint_base}/{table_entity}")
                  .option("cloudFiles.schemaEvolutionMode", "rescue")
                  .option("header", "true")
                  .schema(schema_obj)
                  .load(f"{landing_zone_base}/{target_path}/"))
        
        pipeline_id = spark.conf.get("spark.databricks.clusterUsageTags.pipelineId", "dev_rio_bronze_dynamic")
        
        return add_bronze_metadata(df_raw, pipeline_id=pipeline_id)
    
    return create_table

for entity, conf in ingestion_config.items():
    generate_bronze_table(entity, conf)