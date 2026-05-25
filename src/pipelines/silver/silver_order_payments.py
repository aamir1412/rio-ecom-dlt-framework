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
# 6. ORDER PAYMENTS DOMAIN (SCD TYPE 1)
# ==========================================
@dlt.view(
    name="silver_order_payments_stg",
    comment="Transient view staging cleaned order payments data."
)
@dlt.expect("valid_composite_pk", "order_id IS NOT NULL AND payment_sequential IS NOT NULL")
@dlt.expect("positive_value", "payment_value > 0")
def create_silver_order_payments_stg():
    transformation_rules = {
        "payment_value": col("payment_value").cast("double"),
        "payment_installments": col("payment_installments").cast("int"),
        "payment_sequential": col("payment_sequential").cast("int")
    }
    
    df_casted = (
        read_bronze_stream("bronze_order_payments")
        .withColumns(transformation_rules)
    )
    return apply_silver_metadata(df_casted)

dlt.create_streaming_table(
    name="silver_order_payments",
    comment="SCD Type 1 Order Payments Fact Dimension.",
    table_properties={"quality": "silver", "delta.enableChangeDataFeed": "true"}
)

dlt.apply_changes(
    target="silver_order_payments",
    source="silver_order_payments_stg",
    keys=["order_id", "payment_sequential"],
    sequence_by=col("_bronze_source_file_modified"), 
    stored_as_scd_type=1
)