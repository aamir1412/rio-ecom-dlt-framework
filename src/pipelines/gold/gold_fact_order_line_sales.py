"""
Gold Domain: Order Line Sales Fact
Denormalized Star Schema fact table optimized for granular revenue, logistics, and geographic GMV tracking.
"""

import sys
import os

# 1. Path Resolution & Environment Configuration
# Ensures custom source modules parse correctly on both workspace and isolated cluster runtimes.
try:    
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()    
    project_root = f"/Workspace{notebook_path.split('/src/')[0]}"
except Exception:    
    current_dir = os.getcwd()
    project_root = current_dir.split('/src/')[0] if '/src/' in current_dir else current_dir

if project_root not in sys.path:
    sys.path.append(project_root)    



import dlt
from pyspark.sql.functions import col, to_date, when, lit
from src.shared.spark_io import read_published_silver


# 2. DLT Fact Table Definition
# Registers the target granular fact table into the DLT graph, applying multi-dimensional 
# Z-Ordering to accelerate common BI filtering configurations.
@dlt.table(
    name="gold_fact_order_line_sales",
    comment="Granular line-item fact table supporting executive GMV, AOV, and logistics reporting.",
    cluster_by=["order_date", "product_id", "customer_id"],
    table_properties={
        "quality": "gold",
        "delta.enableDeletionVectors": "true"
    }
)
def create_gold_fact_order_line_sales():
    
    # 3. Source Ingestion
    # Imports production-ready silver entities via centralized I/O handlers.
    df_orders = read_published_silver("silver_orders")
    df_items = read_published_silver("silver_order_items")
    
    # 4. Granular Denormalization Phase
    # Maps order headers to discrete item lines. An inner join enforces strict relational 
    # integrity, filtering out orphaned items or empty orders before calculation.
    df_fact = (
        df_items.alias("i")
        .join(df_orders.alias("o"), col("i.order_id") == col("o.order_id"), "inner")
        .select(            
            col("i.order_id"),
            col("i.order_item_id"),                        
            col("o.customer_id"),
            col("i.seller_id"),
            col("i.product_id"),                        
            col("o.order_purchase_timestamp"),            
            to_date(col("o.order_purchase_timestamp")).alias("order_date"),                    
            col("o.order_status"),                        
            col("i.price").alias("item_price"),
            col("i.freight_value")
        )
    )
    
    # 5. Financial Metric Derivation & Revenue Gate
    # Derives consolidated line values and introduces a standardized business rule to isolate 
    # recognized revenue from pending/canceled order states.
    return (
        df_fact
        .withColumn(
            "total_line_value", 
            col("item_price") + col("freight_value")
        )
        .withColumn(
            "is_recognized_revenue",
            when(col("order_status").isin("delivered", "shipped", "invoiced"), lit(True))
            .otherwise(lit(False))
        )
    )