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
# 2. SELLERS DOMAIN (SCD TYPE 1)
# ==========================================
@dlt.view(
    name="silver_sellers_stg",
    comment="Transient view staging cleaned seller data."
)
@dlt.expect_or_drop("valid_pk", "seller_id IS NOT NULL")
@dlt.expect_all({
    "valid_state_length": "length(seller_state) = 2",
    "valid_zip_numeric": "seller_zip_code_prefix RLIKE '^[0-9]+$'"
})
def create_silver_sellers_stg():
    transformation_rules = {
        "seller_zip_code_prefix": lpad(col("seller_zip_code_prefix"), 5, "0"),
        "seller_city": initcap(col("seller_city")),
        "seller_state": upper(col("seller_state"))
    }
    
    df_normalized = (
        read_bronze_stream("bronze_sellers")
        .withColumns(transformation_rules)
    )
    return apply_silver_metadata(df_normalized)

dlt.create_streaming_table(
    name="silver_sellers",
    comment="SCD Type 1 Seller Master Dimension (Static Reference).",
    table_properties={"quality": "silver", "delta.enableChangeDataFeed": "true"}
)

dlt.apply_changes(
    target="silver_sellers",
    source="silver_sellers_stg",
    keys=["seller_id"],
    sequence_by=col("_bronze_source_file_modified"), 
    stored_as_scd_type=1
)