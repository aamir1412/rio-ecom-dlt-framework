"""
Gold Presentation Domain: Executive Sales Performance OBT
Materializes a fully denormalized One Big Table (OBT) layout.
Optimized via Liquid Clustering for sub-second BI rendering.
"""

import sys
import os

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
from src.shared.spark_io import read_published_gold


@dlt.table(
    name="gold_sales_performance_obt",
    comment="Materialized OBT caching denormalized Sales and FinOps metrics for BI consumption.",
    # Liquid Clustering explicitly declared as a native DLT parameter (requires DBR 13.3+)
    cluster_by=["order_date", "product_category"],
    table_properties={
        "quality": "gold"
    }
)
def create_gold_obt_sales_performance():
    
    # Ingest the modular physical Gold assets utilizing the dedicated Gold schema reader
    df_line_sales = read_published_gold("gold_fact_order_line_sales")
    df_products = read_published_gold("gold_dim_products")
    df_reconciliation = read_published_gold("gold_fact_financial_reconciliation")
    
    # Compile the OBT execution graph
    return (
        df_line_sales.alias("f")
        .join(
            df_products.alias("p"), 
            col("f.product_id") == col("p.product_id"), 
            "inner"
        )
        .join(
            df_reconciliation.alias("r"), 
            col("f.order_id") == col("r.order_id"), 
            "inner"
        )
        .select(
            # Temporal Axes
            col("f.order_date"),
            col("f.order_purchase_timestamp"),
            
            # Product Metrics & Categories
            col("p.product_category_name_english").alias("product_category"),
            col("p.weight_class"),
            col("f.product_id"),
            
            # Line Ingestion Identity
            col("f.order_id"),
            col("f.order_item_id"),
            col("f.order_status"),
            
            # Operational & Governance Routing Flags
            col("f.is_recognized_revenue"),
            col("r.is_orphaned_revenue"),
            
            # Granular Financial Measures (Line Grain - Safe for SUM)
            col("f.item_price"),
            col("f.freight_value"),
            col("f.total_line_value"),
            
            # Aggregated Order Ledger Diagnostics (Order Grain - Requires DISTINCT/AVG in BI)
            col("r.total_order_value"),
            col("r.total_payment_collected"),
            col("r.ledger_variance"),
            col("r.total_installments")
        )
    )