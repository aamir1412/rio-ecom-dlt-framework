"""
Silver Domain: Order Items
Executes SCD Type 1 tracking for transactional line-item attributes.
"""

import sys
import os

# 1. Path Resolution & Core Dependencies
# Resolves the project root directory and appends it to sys.path to ensure custom 
# utilities are discoverable across interactive notebooks and isolated DLT runtimes.
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
# Enforces positive numerical boundaries for financial values and drops invalid keys.
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


# 3. Target Materialization Declaration
# Provisions the physical target streaming infrastructure with Change Data Feed (CDF) 
# activated, enabling downstream micro-batch consumption of updates.
dlt.create_streaming_table(
    name="silver_order_items",
    comment="SCD Type 1 Order Items Fact Dimension.",
    cluster_by=["order_id", "order_item_id"],
    table_properties={
        "quality": "silver", 
        "delta.enableChangeDataFeed": "true",
        "delta.enableDeletionVectors": "true"
    }
)


# 4. SCD Type 1 Upsert Engine Execution
# Executes the slowly changing dimension type 1 engine. Applies an upsert pattern 
# using a composite primary key to update records in place without tracking history.
dlt.apply_changes(
    target="silver_order_items",
    source="silver_order_items_stg",
    # Olist data structure mandates a composite key; failure to define this overwrites line items
    keys=["order_id", "order_item_id"],
    # Utilizes hardware-level file timestamps to guarantee deterministic conflict resolution
    sequence_by=col("_bronze_source_file_modified"), 
    stored_as_scd_type=1
)