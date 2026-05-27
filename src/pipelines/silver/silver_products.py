"""
Silver Domain: Products
Executes SCD Type 1 tracking and static dimensional enrichment for the product catalog.
"""

import sys
import os

# 1. Environment & Path Resolution
# Resolves the underlying project root directory and appends it to sys.path to ensure custom 
# engineering utilities resolve correctly across interactive notebooks and isolated DLT runtimes.
try:    
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()    
    project_root = f"/Workspace{notebook_path.split('/src/')[0]}"
except Exception:
    current_dir = os.getcwd()
    project_root = current_dir.split('/src/')[0] if '/src/' in current_dir else current_dir

if project_root not in sys.path:
    sys.path.append(project_root) 

import dlt
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast
from src.shared.spark_io import read_bronze_stream
from src.shared.transformation import rename_columns, cast_columns
from src.shared.audit import apply_silver_metadata

# Dynamic environment resolution required for the static translation join
spark = SparkSession.builder.getOrCreate()
# source_catalog = spark.conf.get("source_catalog", "cat_ecom_dev")
source_catalog = spark.conf.get("fw.catalog_name")


# 2. Cleansing, Enrichment & Quality Staging View
# Instantiates a transient staging view bound by strict data quality expectations.
# Drops unmapped keys or negative physical attributes, drops non-analytical columns, 
# and dynamically binds language translations using a broadcast hash join.
@dlt.view(
    name="silver_products_stg",
    comment="Transient view staging cleansed, structurally casted, and translated product data."
)
# Quality gates: Guarantee core dimensional integrity and physical constraints
@dlt.expect_or_drop("valid_pk", "product_id IS NOT NULL")
@dlt.expect_or_drop("valid_weight", "product_weight_g >= 0")
def create_silver_products_stg():
    
    # Ingest the static translation dictionary for late-binding enrichment
    df_translations = dlt.read(f"{source_catalog}.bronze.bronze_product_translation").select(
        "product_category_name",
        "product_category_name_english"
    )
    
    # Ingest streaming payload using the decoupled catalog resolver
    df_raw = read_bronze_stream("bronze_products")
    
    # Explicitly purge non-analytical columns to optimize storage and compute footprint
    df_pruned = df_raw.drop(
        "product_name_lenght", 
        "product_description_lenght"
    )
    
    # Apply structural data typing to physical dimensions for downstream Gold aggregations
    type_mapping = {
        "product_weight_g": "double",
        "product_length_cm": "double",
        "product_height_cm": "double",
        "product_width_cm": "double"
    }
    df_casted = cast_columns(df_pruned, type_mapping)
    
    # Broadcast the static translation table to all worker nodes to eliminate shuffle partitions
    df_enriched = df_casted.join(
        broadcast(df_translations),
        on="product_category_name",
        how="left"
    )
    
    # Inject standard Silver lineage timestamps and purge Bronze operational artifacts
    return apply_silver_metadata(df_enriched)


# 3. Target Materialization Declaration
# Provisions the physical target streaming infrastructure with Change Data Feed (CDF) 
# activated, enabling downstream Gold consumption views to capture master catalog mutations incrementally.
dlt.create_streaming_table(
    name="silver_products",
    comment="SCD Type 1 Product Master Dimension.",
    cluster_by=["product_id"],
    table_properties={
        "quality": "silver", 
        "delta.enableChangeDataFeed": "true",
        "delta.enableDeletionVectors": "true"
    }
)

# 4. SCD Type 1 Upsert Engine Execution
# Executes the slowly changing dimension type 1 engine. Synchronizes dimensions in place 
# using the product_id primary key to overwrite changes without tracking historical versions.
dlt.apply_changes(
    target="silver_products",
    source="silver_products_stg",
    keys=["product_id"],
    # Utilizes hardware-level file timestamps to guarantee deterministic conflict resolution
    sequence_by=col("_bronze_source_file_modified"), 
    stored_as_scd_type=1
)