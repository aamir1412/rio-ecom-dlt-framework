"""
Gold Domain: Products Dimension
Materialized dimension table supporting localized and translated merchandising performance metrics.
"""

import sys
import os

# 1. Environment & Path Resolution
# Resolves and appends project root to sys.path to enable custom module imports across local and DLT clusters.
try:    
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()    
    project_root = f"/Workspace{notebook_path.split('/src/')[0]}"
except Exception:    
    current_dir = os.getcwd()
    project_root = current_dir.split('/src/')[0] if '/src/' in current_dir else current_dir

if project_root not in sys.path:
    sys.path.append(project_root)      


import dlt
from pyspark.sql.functions import col, when, lit
from src.shared.spark_io import read_published_silver

# 2. Gold Dimension Materialization
# Defines the DLT target with Z-Order clustering to optimize downstream BI query performance on categorical columns.
@dlt.table(
    name="gold_dim_products",
    comment="Materialized Product Dimension with late-bound English translations.",
    cluster_by=["product_id", "product_category_name_english"],
    table_properties={
        "quality": "gold",
        "delta.enableDeletionVectors": "true"
    }
)
def create_gold_dim_products():
    
    # 3. Upstream Ingestion
    # Retrieves validated silver-layer records via standardized IO wrapper.
    df_products = read_published_silver("silver_products")
    
    # 4. Dimensional Transformations & Business Logic
    # Standardizes missing translations and categorizes continuous weight data into discrete analytical tiers.
    return (
        df_products
        .withColumn(
            "product_category_name_english",
            when(col("product_category_name_english").isNull(), lit("unknown_or_untranslated"))
            .otherwise(col("product_category_name_english"))
        )
        .withColumn(
            "weight_class",
            when(col("product_weight_g") < 1000, lit("Lightweight (<1kg)"))
            .when((col("product_weight_g") >= 1000) & (col("product_weight_g") < 5000), lit("Standard (1-5kg)"))
            .otherwise(lit("Heavyweight (>5kg)"))
        )
    )