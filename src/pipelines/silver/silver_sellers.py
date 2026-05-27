"""
Silver Domain: Sellers
Executes SCD Type 1 dimension tracking for merchant geographic attributes.
"""

import sys
import os

# 1. Path Resolution & Environment Configuration
# Resolves the underlying project directory to dynamically append custom engineering 
# utilities to sys.path across interactive and cluster runtime engines.
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
from pyspark.sql.functions import col, lpad, initcap, upper
from src.shared.spark_io import read_bronze_stream
from src.shared.transformation import rename_columns
from src.shared.audit import apply_silver_metadata


# 2. Cleansing, Standardization & Quality Staging View
# Instantiates a transient staging view bound by strict data quality expectations.
# Enforces geographic formatting rules, renames prefixes, and drops unmapped records.
@dlt.view(
    name="silver_sellers_stg",
    comment="Transient view staging cleansed and normalized seller data."
)
# Quality gates: Guarantee relational integrity and valid geographic formatting
@dlt.expect_or_drop("valid_pk", "seller_id IS NOT NULL")
@dlt.expect_all({
    "valid_state_length": "length(state) = 2",
    "valid_zip_numeric": "zip_code RLIKE '^[0-9]+$'"
})
def create_silver_sellers_stg():
    
    # Ingests payload using the dynamic catalog resolver to maintain CI/CD isolation
    df_raw = read_bronze_stream("bronze_sellers")
    
    # Strip redundant entity prefixes to optimize downstream Gold layer SQL readability
    rename_mapping = {
        "seller_zip_code_prefix": "zip_code",
        "seller_city": "city",
        "seller_state": "state"
    }
    df_renamed = rename_columns(df_raw, rename_mapping)
    
    # Standardize string formatting to prevent case-sensitivity mismatches during Gold layer joins
    transformation_rules = {
        "zip_code": lpad(col("zip_code"), 5, "0"),
        "city": initcap(col("city")),
        "state": upper(col("state"))
    }
    df_normalized = df_renamed.withColumns(transformation_rules)
    
    # Inject standard Silver lineage timestamps and purge Bronze operational artifacts
    return apply_silver_metadata(df_normalized)


# 3. Target Materialization Declaration
# Provisions the physical target streaming infrastructure with Change Data Feed (CDF) 
# activated, enabling downstream Gold consumption views to capture master catalog mutations incrementally.
dlt.create_streaming_table(
    name="silver_sellers",
    comment="SCD Type 1 Seller Master Dimension (Static Reference).",
    cluster_by=["seller_id"],
    table_properties={
        "quality": "silver", 
        "delta.enableChangeDataFeed": "true",
        "delta.enableDeletionVectors": "true"
    }
)


# 4. SCD Type 1 Upsert Engine Execution
# Executes the slowly changing dimension type 1 engine. Synchronizes dimensions in place 
# using the seller_id primary key to overwrite changes without tracking historical versions.
dlt.apply_changes(
    target="silver_sellers",
    source="silver_sellers_stg",
    keys=["seller_id"],
    # Utilizes hardware-level file timestamps to guarantee deterministic conflict resolution
    sequence_by=col("_bronze_source_file_modified"), 
    stored_as_scd_type=1
)