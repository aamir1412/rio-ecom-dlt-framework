# src/pipelines/02_silver.py

import dlt
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from src.shared.audit import apply_silver_metadata
from src.config.silver_config import SILVER_CONFIG

spark = SparkSession.builder.getOrCreate()
source_catalog = spark.conf.get("source_catalog", "cat_ecom_dev")

def generate_silver_node(entity_name: str, config: dict):
    """
    Dynamically generates DLT Views, Tables, and CDC Engines via Metadata.
    """
    
    # 1. Core Staging Logic Closure
    def create_staging():
        df_raw = dlt.read_stream(f"{source_catalog}.bronze.bronze_{entity_name}")
        
        # Route to custom function if complex DAG required, otherwise use dictionary rules
        if "transform_fn" in config:
            df_transformed = config["transform_fn"](df_raw, source_catalog)
        else:
            df_transformed = df_raw.withColumns(config.get("transform_rules", {}))
            
        return apply_silver_metadata(df_transformed)

    # 2. Dynamic Decorator Application (Quality Gates)
    staging_func = create_staging
    
    if "expect_or_drop" in config:
        staging_func = dlt.expect_all_or_drop(config["expect_or_drop"])(staging_func)
        
    if "expect" in config:
        staging_func = dlt.expect_all(config["expect"])(staging_func)
        
    # Apply standard view wrapper
    staging_func = dlt.view(
        name=f"silver_{entity_name}_stg",
        comment=f"Transient view staging cleaned {entity_name} data."
    )(staging_func)

    # 3. Target Table Instantiation
    dlt.create_streaming_table(
        name=f"silver_{entity_name}",
        comment=f"SCD Type {config['scd_type']} {entity_name} Master Dimension.",
        table_properties={"quality": "silver", "delta.enableChangeDataFeed": "true"}
    )

    # 4. CDC Execution Engine Configuration
    apply_changes_kwargs = {
        "target": f"silver_{entity_name}",
        "source": f"silver_{entity_name}_stg",
        "keys": config["keys"],
        "sequence_by": col("_bronze_source_file_modified"),
        "stored_as_scd_type": config["scd_type"]
    }
    
    # Inject SCD2 historical tracking safely
    if "history_cols" in config:
        apply_changes_kwargs["track_history_column_list"] = config["history_cols"]
        
    dlt.apply_changes(**apply_changes_kwargs)

# ==========================================
# FACTORY EXECUTION
# ==========================================
for entity, configuration in SILVER_CONFIG.items():
    generate_silver_node(entity, configuration)