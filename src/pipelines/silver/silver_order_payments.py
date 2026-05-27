"""
Silver Domain: Order Payments
Executes SCD Type 1 tracking for financial transaction ledgers.
"""

import sys
import os

# 1. Path Resolution & Core Dependencies
# Resolves the project root directory and appends it to sys.path to ensure custom 
# engineering modules resolve correctly across workspace and cluster runtimes.
try:
    # Attempt to grab the path from the Databricks context
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    # /Workspace/Shared/.bundle/rio-ecom-dlt-framework/dev/files/src/pipelines/silver/silver_products.py
    # We split on 'src' to dynamically find the root regardless of how deep we are
    project_root = f"/Workspace{notebook_path.split('/src/')[0]}"
except Exception:
    # Fallback if dbutils is unavailable (e.g., local testing)
    current_dir = os.getcwd()
    project_root = current_dir.split('/src/')[0] if '/src/' in current_dir else current_dir

if project_root not in sys.path:
    sys.path.append(project_root) 

import dlt
from pyspark.sql.functions import col
from src.shared.spark_io import read_bronze_stream
from src.shared.transformation import cast_columns
from src.shared.audit import apply_silver_metadata


# 2. Cleansing, Schema Enforcement & Quality Staging View
# Instantiates a transient staging view bound by soft data quality expectations (@dlt.expect).
# Records violating financial metrics or key boundaries are tracked via DLT event logs 
# instead of being dropped, preserving the raw transaction history for down-stream auditing.
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


# 3. Target Materialization Declaration
# Provisions the physical target streaming infrastructure with Change Data Feed (CDF) 
# activated, enabling downstream financial metrics to capture and track incremental adjustments.
dlt.create_streaming_table(
    name="silver_order_payments",
    comment="SCD Type 1 Order Payments Fact Dimension.",
    table_properties={
        "quality": "silver", 
        "delta.enableChangeDataFeed": "true"
    }
)


# 4. SCD Type 1 Upsert Engine Execution
# Executes the slowly changing dimension type 1 engine. Applies an upsert pattern 
# using a composite primary key to update transaction values in place based on file metadata.
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