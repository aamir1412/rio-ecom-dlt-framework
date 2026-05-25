"""
Silver Domain: Order Items
Executes SCD Type 1 tracking for transactional line-item attributes.
"""

import dlt
from pyspark.sql.functions import col
from src.shared.spark_io import read_bronze_stream
from src.shared.transformation import cast_columns
from src.shared.audit import apply_silver_metadata


@dlt.view(
    name="silver_order_items_stg",
    comment="Transient view staging cleansed and structurally casted order items data."
)
# Quality gates: Guarantee composite relational integrity and prevent negative financial anomalies
@dlt.expect_or_drop("valid_pk", "order_id IS NOT NULL AND order_item_id IS NOT NULL")
@dlt.expect_or_drop("positive_price", "price >= 0")
@dlt.expect_or_drop("positive_freight", "freight_value >= 0")
def create_silver_order_items_stg():
    
    # Ingests payload using the dynamic catalog resolver to maintain CI/CD isolation
    df_raw = read_bronze_stream("bronze_order_items")
    
    # Apply structural data typing to enforce analytical strictness for downstream aggregations
    type_mapping = {
        "price": "double",
        "freight_value": "double",
        "shipping_limit_date": "timestamp",
        "order_item_id": "int"
    }
    df_casted = cast_columns(df_raw, type_mapping)
    
    # Inject standard Silver lineage timestamps and purge Bronze operational artifacts
    return apply_silver_metadata(df_casted)


# Materialized structure for the SCD1 target. 
# CDF is explicitly enabled so Gold layer fact tables can consume updates incrementally.
dlt.create_streaming_table(
    name="silver_order_items",
    comment="SCD Type 1 Order Items Fact Dimension.",
    table_properties={
        "quality": "silver", 
        "delta.enableChangeDataFeed": "true"
    }
)


# RocksDB-backed merge execution engine.
dlt.apply_changes(
    target="silver_order_items",
    source="silver_order_items_stg",
    # Olist data structure mandates a composite key; failure to define this overwrites line items
    keys=["order_id", "order_item_id"],
    # Utilizes hardware-level file timestamps to guarantee deterministic conflict resolution
    sequence_by=col("_bronze_source_file_modified"), 
    stored_as_scd_type=1
)