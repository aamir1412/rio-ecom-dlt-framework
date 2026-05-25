"""
Silver Domain: Order Payments
Executes SCD Type 1 tracking for financial transaction ledgers.
"""

import dlt
from pyspark.sql.functions import col
from src.shared.spark_io import read_bronze_stream
from src.shared.transformation import cast_columns
from src.shared.audit import apply_silver_metadata


@dlt.view(
    name="silver_order_payments_stg",
    comment="Transient view staging cleansed financial payment data."
)
# Soft expectations (quarantine) are used here instead of hard drops (expect_or_drop).
# This prevents silent data destruction of corrupted financial rows, allowing them to 
# flow into Silver for FinOps auditing while downstream Gold ledgers handle reconciliation.
@dlt.expect("valid_composite_pk", "order_id IS NOT NULL AND payment_sequential IS NOT NULL")
@dlt.expect("positive_value", "payment_value > 0")
def create_silver_order_payments_stg():
    
    # Ingests payload using the dynamic catalog resolver to maintain CI/CD isolation
    df_raw = read_bronze_stream("bronze_order_payments")
    
    # Apply structural data typing to enforce precision for downstream financial aggregations
    type_mapping = {
        "payment_value": "double",
        "payment_installments": "int",
        "payment_sequential": "int"
    }
    df_casted = cast_columns(df_raw, type_mapping)
    
    # Inject standard Silver lineage timestamps and purge Bronze operational artifacts
    return apply_silver_metadata(df_casted)


# Materialized structure for the SCD1 target. 
# CDF is explicitly enabled so Gold layer financial dashboards can consume incremental revenue updates.
dlt.create_streaming_table(
    name="silver_order_payments",
    comment="SCD Type 1 Order Payments Fact Dimension.",
    table_properties={
        "quality": "silver", 
        "delta.enableChangeDataFeed": "true"
    }
)


# RocksDB-backed merge execution engine.
dlt.apply_changes(
    target="silver_order_payments",
    source="silver_order_payments_stg",
    # CRITICAL: A single order frequently contains multiple payment methods (e.g., Voucher + Credit Card).
    # payment_sequential is required in the composite key to guarantee financial row uniqueness.
    keys=["order_id", "payment_sequential"],
    # Utilizes hardware-level file timestamps to guarantee deterministic conflict resolution
    sequence_by=col("_bronze_source_file_modified"), 
    stored_as_scd_type=1
)