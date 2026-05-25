# src/pipelines/01_bronze.py

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