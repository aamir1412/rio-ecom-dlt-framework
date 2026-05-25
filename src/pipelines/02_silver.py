# src/pipelines/02_silver.py

import dlt
from pyspark.sql.functions import col, lpad, initcap, upper
from src.shared.audit import apply_silver_metadata

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
        dlt.read_stream("cat_ecom_dev.bronze.bronze_customers")
        .withColumns(transformation_rules)
    )
    
    return apply_silver_metadata(df_normalized)


dlt.create_streaming_table(
    name="silver_customers",
    comment="SCD Type 1 Customer Master Dimension.",
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true" 
    }
)

# EXECUTION ENGINE: DLT AUTO CDC (SCD Type 2)
dlt.apply_changes(
    target="silver_customers",
    source="silver_customers_stg",
    keys=["customer_id"],
    # Deterministic sequencing via hardware-level file timestamps
    sequence_by=col("_bronze_source_file_modified"), 
    stored_as_scd_type=2,
    # MANDATORY: Explicitly define which columns trigger a historical version
    track_history_column_list=[
        "customer_zip_code_prefix", 
        "customer_city", 
        "customer_state"
    ]
)