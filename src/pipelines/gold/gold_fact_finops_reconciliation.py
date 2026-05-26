"""
Gold Domain: FinOps Reconciliation Fact
Denormalized Star Schema optimized for ledger balancing and automated revenue anomaly detection.
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
from pyspark.sql.functions import col, sum, round, when, lit, coalesce
from src.shared.spark_io import read_published_silver


@dlt.table(
    name="gold_fact_financial_reconciliation",
    comment="Materialized View for FinOps ledger balancing. Denormalized to the order grain.",
    table_properties={
        "quality": "gold",
        # Optimize underlying Parquet clustering for the most frequent BI dashboard filtering patterns
        "pipelines.autoOptimize.zOrderCols": "order_purchase_timestamp, order_status"
    }
)
def create_gold_fact_financial_reconciliation():
    
    # Read static snapshots of the published Silver layer.
    df_orders = read_published_silver("silver_orders")
    df_items = read_published_silver("silver_order_items")
    df_payments = read_published_silver("silver_order_payments")
    
    # Isolation Phase 1: Pre-aggregate physical product value to the primary order grain.
    # This mathematically guarantees the prevention of Cartesian explosions before joining to the core fact.
    df_items_agg = (
        df_items.groupBy("order_id")
        .agg(
            sum(col("price") + col("freight_value")).alias("total_order_value")
        )
    )
    
    # Isolation Phase 2: Pre-aggregate financial ledger collections to the order grain.
    df_payments_agg = (
        df_payments.groupBy("order_id")
        .agg(
            sum("payment_value").alias("total_payment_collected"),
            sum("payment_installments").alias("total_installments")
        )
    )
    
    # Denormalization Phase: Build the unified analytical Star schema fact.
    df_fact = (
        df_orders.alias("o")
        .join(df_items_agg.alias("i"), col("o.order_id") == col("i.order_id"), "left")
        .join(df_payments_agg.alias("p"), col("o.order_id") == col("p.order_id"), "left")
        .select(
            col("o.order_id"),
            col("o.customer_id"),
            col("o.order_status"),
            col("o.order_purchase_timestamp"),
            coalesce(col("i.total_order_value"), lit(0.0)).alias("total_order_value"),
            coalesce(col("p.total_payment_collected"), lit(0.0)).alias("total_payment_collected"),
            coalesce(col("p.total_installments"), lit(1)).alias("total_installments")
        )
    )
    
    # Business Logic Phase: Inject actionable FinOps KPIs and automated risk flags.
    # These columns empower downstream BI alerts (e.g., triggering a Slack webhook for orphaned revenue) 
    # rather than forcing analysts to manually calculate variances in PowerBI.
    return (
        df_fact
        .withColumn(
            "ledger_variance", 
            round(col("total_payment_collected") - col("total_order_value"), 2)
        )
        .withColumn(
            "is_orphaned_revenue",
            when((col("order_status") == "delivered") & (col("total_payment_collected") == 0), lit(True))
            .otherwise(lit(False))
        )
    )