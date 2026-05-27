"""
Silver Domain: Orders
Executes SCD Type 1 tracking for core transactional fact records.
"""

import sys
import os

# 1. Workspace & Runtime Path Resolution
# Resolves the project root directory and appends it to sys.path to ensure custom 
# source utilities are discoverable across interactive notebooks and isolated DLT runtimes.
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
# Instantiates a transient staging view bound by strict data quality expectations.
# Enforces a valid state-machine lifecycle checklist and drops unmapped records.
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


# 3. Target Materialization Declaration
# Provisions the physical target streaming infrastructure with Change Data Feed (CDF) 
# activated, enabling downstream Gold consumption views to capture lifecycle shifts incrementally.
dlt.create_streaming_table(
    name="silver_orders",
    comment="SCD Type 1 Order Master Fact.",
    table_properties={
        "quality": "silver", 
        "delta.enableChangeDataFeed": "true"
    }
)


# 4. SCD Type 1 Upsert Engine Execution
# Executes the slowly changing dimension type 1 engine. Merges updates into the target 
# based on order_id, updating downstream status modifications (e.g., from shipped to delivered).
dlt.apply_changes(
    target="silver_orders",
    source="silver_orders_stg",
    keys=["order_id"],
    # Utilizes hardware-level file timestamps to guarantee deterministic conflict resolution
    sequence_by=col("_bronze_source_file_modified"), 
    stored_as_scd_type=1
)