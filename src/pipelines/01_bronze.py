import sys
import os
import importlib
import dlt
from pyspark.sql import SparkSession

# 1. Path Injection for Bundle Resolution
try:
    # Resolves correctly during local IDE execution or DABs deployment
    current_dir = os.path.dirname(__file__)
except NameError:
    # Fallback for Databricks UI / Lakeflow Editor (REPL execution)
    current_dir = os.getcwd()

repo_root = os.path.abspath(os.path.join(current_dir, '..', '..'))
if repo_root not in sys.path:
    sys.path.append(repo_root)

from src.shared.audit import add_bronze_metadata

# 2. Load Configuration via Native Module Import (Bypasses File I/O)
from src.config.bronze_config import INGESTION_CONFIG as ingestion_config

spark = SparkSession.builder.getOrCreate()
landing_zone_base = "/Volumes/cat_ecom_dev/raw/vol_landing_zone"
checkpoint_base = "/Volumes/cat_ecom_dev/raw/schema_checkpoints"

# 3. Factory Function for DLT Node Generation
def generate_bronze_table(table_entity: str, config: dict):
    
    # Use Reflection to dynamically import the schema module and object
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
        
        # Extract the Pipeline ID from the Spark cluster's internal usage tags
        pipeline_id = spark.conf.get("spark.databricks.clusterUsageTags.pipelineId", "dev_rio_bronze_dynamic")
        
        return add_bronze_metadata(df_raw, pipeline_id=pipeline_id)
        # return add_bronze_metadata(df_raw, pipeline_id=dlt.current.pipeline_id())
    
    return create_table

# 4. Execute the factory for all configured tables
for entity, conf in ingestion_config.items():
    generate_bronze_table(entity, conf)