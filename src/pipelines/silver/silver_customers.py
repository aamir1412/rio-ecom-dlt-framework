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
# 1. CUSTOMERS DOMAIN (SCD TYPE 2)
# ==========================================
@dlt.view(
    name="silver_customers_stg",
    comment="Transient view staging cleaned customer data."
)
@dlt.expect_or_drop("valid_pk", "customer_id IS NOT NULL")
@dlt.expect_all({
    "valid_state_length": "length(customer_state) = 2",
    "valid_zip_numeric": "customer_zip_code_prefix RLIKE '^[0-9]+$'"
})
def create_silver_customers_stg():
    transformation_rules = {
        "customer_zip_code_prefix": lpad(col("customer_zip_code_prefix"), 5, "0"),
        "customer_city": initcap(col("customer_city")),
        "customer_state": upper(col("customer_state"))
    }
    
    df_normalized = (
        read_bronze_stream("bronze_customers")
        .withColumns(transformation_rules)
    )
    return apply_silver_metadata(df_normalized)

dlt.create_streaming_table(
    name="silver_customers",
    comment="SCD Type 2 Customer Master Dimension.",
    table_properties={"quality": "silver", "delta.enableChangeDataFeed": "true"}
)

dlt.apply_changes(
    target="silver_customers",
    source="silver_customers_stg",
    keys=["customer_id"],
    sequence_by=col("_bronze_source_file_modified"), 
    stored_as_scd_type=2,
    track_history_column_list=["customer_zip_code_prefix", "customer_city", "customer_state"]
)
