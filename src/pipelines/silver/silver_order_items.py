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
# 5. ORDER ITEMS DOMAIN (SCD TYPE 1)
# ==========================================
@dlt.view(
    name="silver_order_items_stg",
    comment="Transient view staging cleaned order items data."
)
@dlt.expect_or_drop("valid_pk", "order_id IS NOT NULL AND order_item_id IS NOT NULL")
@dlt.expect_or_drop("positive_price", "price >= 0")
@dlt.expect_or_drop("positive_freight", "freight_value >= 0")
def create_silver_order_items_stg():
    transformation_rules = {
        "price": col("price").cast("double"),
        "freight_value": col("freight_value").cast("double"),
        "shipping_limit_date": col("shipping_limit_date").cast("timestamp"),
        "order_item_id": col("order_item_id").cast("int")
    }
    
    df_casted = (
        read_bronze_stream("bronze_order_items")
        .withColumns(transformation_rules)
    )
    return apply_silver_metadata(df_casted)

dlt.create_streaming_table(
    name="silver_order_items",
    comment="SCD Type 1 Order Items Fact Dimension.",
    table_properties={"quality": "silver", "delta.enableChangeDataFeed": "true"}
)

dlt.apply_changes(
    target="silver_order_items",
    source="silver_order_items_stg",
    keys=["order_id", "order_item_id"],
    sequence_by=col("_bronze_source_file_modified"), 
    stored_as_scd_type=1
)