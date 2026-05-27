"""
Silver Domain: Customers
Executes SCD Type 2 dimension tracking for geographic customer attributes.
"""

import sys
import os

# 1. Workspace & Runtime Path Resolution
# Resolves the underlying project directory to dynamically append custom engineering 
# utilities to sys.path across interactive and cluster runtime engines.
try:    
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()    
    project_root = f"/Workspace{notebook_path.split('/src/')[0]}"
except Exception:    
    current_dir = os.getcwd()
    project_root = current_dir.split('/src/')[0] if '/src/' in current_dir else current_dir    

if project_root not in sys.path:
    sys.path.append(project_root) 

import dlt
from pyspark.sql.functions import col, lpad, initcap, upper
from src.shared.spark_io import read_bronze_stream
from src.shared.transformation import rename_columns
from src.shared.audit import apply_silver_metadata


# 2. Cleansing & Quality Gates Staging View
# Instantiates a transient staging view bound by strict data quality expectations. 
# Records violating critical primary key constraints or schema length specs are dropped.
@dlt.view(
    name="silver_customers_stg",
    comment="Transient view staging cleansed and normalized customer data."
)
@dlt.expect_or_drop("valid_pk", "customer_id IS NOT NULL")
@dlt.expect_all({
    "valid_state_length": "length(state) = 2",
    "valid_zip_numeric": "zip_code RLIKE '^[0-9]+$'"
})
def create_silver_customers_stg():
    
    # Ingests payload using the dynamic catalog resolver to maintain CI/CD isolation
    df_raw = read_bronze_stream("bronze_customers")
    
    # Strip redundant entity prefixes to optimize downstream Gold layer SQL readability
    rename_mapping = {
        "customer_zip_code_prefix": "zip_code",
        "customer_city": "city",
        "customer_state": "state"
    }
    df_renamed = rename_columns(df_raw, rename_mapping)
    
    # Standardize string formatting to prevent case-sensitivity mismatches during joins
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
# activated, enabling downstream micro-batch consumption of structural historical updates.
dlt.create_streaming_table(
    name="silver_customers",
    comment="SCD Type 2 Customer Master Dimension.",
    cluster_by=["customer_id"],
    table_properties={
        "quality": "silver", 
        "delta.enableChangeDataFeed": "true",
        "delta.enableDeletionVectors": "true"
    }
)

# 4. SCD Type 2 Merge Engine Execution
# Executes the slowly changing dimension type 2 engine. Automatically manages operational 
# tracking columns (__start_at, __end_at) to log mutable geographic attributes.
dlt.apply_changes(
    target="silver_customers",
    source="silver_customers_stg",
    keys=["customer_id"],
    # Utilizes hardware-level file timestamps to guarantee deterministic conflict resolution
    sequence_by=col("_bronze_source_file_modified"), 
    stored_as_scd_type=2,
    # Explicit isolation prevents pipeline metadata updates from minting false historical versions
    track_history_column_list=["zip_code", "city", "state"]
)