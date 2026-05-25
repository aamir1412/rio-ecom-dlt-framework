# src/pipelines/02_silver.py

import dlt
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lpad, initcap, upper, broadcast
from src.shared.audit import apply_silver_metadata

# ==========================================
# 0. GLOBAL PIPELINE CONFIGURATION & UTILS
# ==========================================
spark = SparkSession.builder.getOrCreate()
source_catalog = spark.conf.get("source_catalog", "cat_ecom_dev")

def read_bronze_stream(table_name: str):
    """
    Modular utility to standardizes environment-aware Bronze layer reads.
    Guarantees CI/CD promotion safety across DEV/PROD catalogs.
    """
    return dlt.read_stream(f"{source_catalog}.bronze.{table_name}")

 
# ==========================================
# 3. PRODUCTS DOMAIN (SCD TYPE 1)
# ==========================================
@dlt.view(
    name="silver_products_stg",
    comment="Transient view staging cleaned and translated product data."
)
@dlt.expect_or_drop("valid_pk", "product_id IS NOT NULL")
@dlt.expect_or_drop("valid_weight", "product_weight_g >= 0")
def create_silver_products_stg():
    df_translations = dlt.read(f"{source_catalog}.bronze.bronze_product_translation").select(
        "product_category_name",
        "product_category_name_english"
    )
    
    transformation_rules = {
        "product_name_length": col("product_name_lenght"),
        "product_description_length": col("product_description_lenght"),
        "product_weight_g": col("product_weight_g").cast("double"),
        "product_length_cm": col("product_length_cm").cast("double"),
        "product_height_cm": col("product_height_cm").cast("double"),
        "product_width_cm": col("product_width_cm").cast("double")
    }
    
    df_products = (
        read_bronze_stream("bronze_products")
        .withColumns(transformation_rules)
        .drop("product_name_lenght", "product_description_lenght")
    )
    
    df_enriched = df_products.join(
        broadcast(df_translations),
        on="product_category_name",
        how="left"
    )
    return apply_silver_metadata(df_enriched)

dlt.create_streaming_table(
    name="silver_products",
    comment="SCD Type 1 Product Master Dimension.",
    table_properties={"quality": "silver", "delta.enableChangeDataFeed": "true"}
)

dlt.apply_changes(
    target="silver_products",
    source="silver_products_stg",
    keys=["product_id"],
    sequence_by=col("_bronze_source_file_modified"), 
    stored_as_scd_type=1
)