"""
Silver Domain: Orders
Executes SCD Type 1 tracking for core transactional fact records.
"""

import dlt
from pyspark.sql.functions import col
from src.shared.spark_io import read_bronze_stream
from src.shared.transformation import cast_columns
from src.shared.audit import apply_silver_metadata


@dlt.view(
    name="silver_orders_stg",
    comment="Transient view staging cleansed order data with validated temporal structures."
)
# Quality gates: Guarantee core transactional integrity and enforce state machine lifecycles
@dlt.expect_or_drop("valid_pk", "order_id IS NOT NULL")
@dlt.expect_or_drop(
    "valid_status", 
    "order_status IN ('delivered', 'shipped', 'canceled', 'invoiced', 'processing', 'unavailable', 'approved', 'created')"
)
def create_silver_orders_stg():
    
    # Ingests payload using the dynamic catalog resolver to maintain CI/CD isolation
    df_raw = read_bronze_stream("bronze_orders")
    
    # Standardize temporal data types to enable downstream time-series aggregations
    type_mapping = {
        "order_purchase_timestamp": "timestamp",
        "order_approved_at": "timestamp",
        "order_delivered_carrier_date": "timestamp",
        "order_delivered_customer_date": "timestamp",
        "order_estimated_delivery_date": "timestamp"
    }
    
    # Leverages single-pass Catalyst projection for optimal graph execution
    df_casted = cast_columns(df_raw, type_mapping)
    
    # Inject standard Silver lineage timestamps and purge Bronze operational artifacts
    return apply_silver_metadata(df_casted)


# Materialized structure for the SCD1 target. 
# CDF is explicitly enabled so Gold layer consumption views trigger incrementally.
dlt.create_streaming_table(
    name="silver_orders",
    comment="SCD Type 1 Order Master Fact.",
    table_properties={
        "quality": "silver", 
        "delta.enableChangeDataFeed": "true"
    }
)


# RocksDB-backed merge execution engine.
dlt.apply_changes(
    target="silver_orders",
    source="silver_orders_stg",
    keys=["order_id"],
    # Utilizes hardware-level file timestamps to guarantee deterministic conflict resolution
    sequence_by=col("_bronze_source_file_modified"), 
    stored_as_scd_type=1
)