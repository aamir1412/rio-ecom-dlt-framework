# src/pipelines/01_bronze.py

import sys
import os
import importlib
import dlt
import uuid
from pyspark.sql import SparkSession
from src.shared.audit import add_bronze_metadata
from src.config.bronze_config import INGESTION_CONFIG as ingestion_config

# 1. Environment & Path Resolution
# Dynamically resolves and appends the project root to sys.path, ensuring custom Python 
# modules resolve correctly in both interactive workspaces and isolated DLT clusters.
try:    
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()    
    project_root = f"/Workspace{notebook_path.split('/src/')[0]}"
except Exception:    
    current_dir = os.getcwd()
    project_root = current_dir.split('/src/')[0] if '/src/' in current_dir else current_dir

if project_root not in sys.path:
    sys.path.append(project_root) 

# 2. Spark & Unity Catalog Setup
# Initializes the Spark context and constructs Unity Catalog Volume paths dynamically 
# using the environment-specific catalog name passed via Spark config.
spark = SparkSession.builder.getOrCreate()
catalog_name = spark.conf.get("fw.catalog_name")

landing_zone_base = f"/Volumes/{catalog_name}/raw/vol_landing_zone"
checkpoint_base = f"/Volumes/{catalog_name}/raw/schema_checkpoints"

# 3. DLT Metaprogramming Factory
# Abstracts Databricks Auto Loader logic to generate streaming tables dynamically. 
# Enforces schema validation via reflection, handles drift by routing to a rescue 
# column, and appends pipeline lineage metadata.
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
        
        pipeline_id = spark.conf.get("spark.databricks.clusterUsageTags.pipelineId", f"{uuid.uuid4().hex}")
        
        return add_bronze_metadata(df_raw, pipeline_id=pipeline_id)
    
    return create_table

# 4. Pipeline DAG Registration
# Iterates over the configuration dictionary to register all tables onto the DLT graph.
for entity, conf in ingestion_config.items():
    generate_bronze_table(entity, conf)