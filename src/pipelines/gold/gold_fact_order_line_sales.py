"""
Gold Domain: Order Line Sales Fact
Denormalized Star Schema fact table optimized for granular revenue, logistics, and geographic GMV tracking.
"""

import sys
import os

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


@dlt.table(
    name="gold_fact_order_line_sales",
    comment="Granular line-item fact table supporting executive GMV, AOV, and logistics reporting.",
    table_properties={
        "quality": "gold",
        # Optimize storage layout for time-series and geographic slicing
        "pipelines.autoOptimize.zOrderCols": "order_date, customer_id, seller_id"
    }
)
def create_gold_fact_order_line_sales():
    
    # Ingest published Silver tables via the cross-pipeline Unity Catalog reader
    df_orders = read_published_silver("silver_orders")
    df_items = read_published_silver("silver_order_items")
    
    # Execute the core fact denormalization.
    # An INNER JOIN is mathematically required here; an order item cannot exist without an order header,
    # and we do not want to record revenue for empty orders.
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
    
    # Execute business logic extensions to prevent downstream BI calculation errors
    return (
        df_fact
        .withColumn(
            "total_line_value", 
            col("item_price") + col("freight_value")
        )
        # Implement a deterministic revenue recognition gate based on physical state
        .withColumn(
            "is_recognized_revenue",
            when(col("order_status").isin("delivered", "shipped", "invoiced"), lit(True))
            .otherwise(lit(False))
        )
    )