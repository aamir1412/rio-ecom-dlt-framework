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
# 4. ORDERS DOMAIN (SCD TYPE 1)
# ==========================================
@dlt.view(
    name="silver_orders_stg",
    comment="Transient view staging cleaned order data with casted timestamps."
)
@dlt.expect_or_drop("valid_pk", "order_id IS NOT NULL")
@dlt.expect_or_drop(
    "valid_status", 
    "order_status IN ('delivered', 'shipped', 'canceled', 'invoiced', 'processing', 'unavailable', 'approved', 'created')"
)
def create_silver_orders_stg():
    timestamp_columns = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date"
    ]
    
    # Dynamic dictionary comprehension for type casting
    transformation_rules = {t_col: col(t_col).cast("timestamp") for t_col in timestamp_columns}
    
    df_casted = (
        read_bronze_stream("bronze_orders")
        .withColumns(transformation_rules)
    )
    return apply_silver_metadata(df_casted)

dlt.create_streaming_table(
    name="silver_orders",
    comment="SCD Type 1 Order Master Fact.",
    table_properties={"quality": "silver", "delta.enableChangeDataFeed": "true"}
)

dlt.apply_changes(
    target="silver_orders",
    source="silver_orders_stg",
    keys=["order_id"],
    sequence_by=col("_bronze_source_file_modified"), 
    stored_as_scd_type=1
)