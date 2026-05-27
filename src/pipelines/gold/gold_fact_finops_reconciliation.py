"""
Gold Domain: FinOps Reconciliation Fact
Denormalized Star Schema optimized for ledger balancing and automated revenue anomaly detection.
""" 

import sys
import os

# 1. Path Resolution
# Appends project root to sys.path to enable custom module resolution. 
# Functions across standard Databricks interactive clusters and isolated DLT runtimes.
try:    
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()    
    project_root = f"/Workspace{notebook_path.split('/src/')[0]}"
except Exception:    
    current_dir = os.getcwd()
    project_root = current_dir.split('/src/')[0] if '/src/' in current_dir else current_dir

if project_root not in sys.path:
    sys.path.append(project_root)     



import dlt
from pyspark.sql.functions import col, sum, round, when, lit, coalesce
from src.shared.spark_io import read_published_silver

# 2. DLT Target Definition
# Materializes the fact table with Z-Order clustering to optimize file skipping 
# on the most frequent downstream temporal and categorical query filters.
@dlt.table(
    name="gold_fact_financial_reconciliation",
    comment="Materialized View for FinOps ledger balancing. Denormalized to the order grain.",
    cluster_by=["order_purchase_timestamp", "order_status"],
    table_properties={
        "quality": "gold",        
        "delta.enableDeletionVectors": "true"
    }
)
def create_gold_fact_financial_reconciliation():
    
    # 3. Silver Ingestion
    # Pulls validated entity tables via standard I/O abstraction.
    df_orders = read_published_silver("silver_orders")
    df_items = read_published_silver("silver_order_items")
    df_payments = read_published_silver("silver_order_payments")
    
    # 4. Dimension Pre-Aggregation
    # Condenses multi-line items and multi-payment records to the primary order grain 
    # prior to joining. This structurally prevents Cartesian explosion during denormalization.
    df_items_agg = (
        df_items.groupBy("order_id")
        .agg(
            sum(col("price") + col("freight_value")).alias("total_order_value")
        )
    )
    
    df_payments_agg = (
        df_payments.groupBy("order_id")
        .agg(
            sum("payment_value").alias("total_payment_collected"),
            sum("payment_installments").alias("total_installments")
        )
    )
    
    # 5. Fact Denormalization
    # Constructs the unified analytical schema. Uses left joins to preserve the 
    # master order record and coalesces nulls to prevent math errors downstream.
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
    
    # 6. FinOps KPI Injection
    # Materializes core financial metrics (variance, orphaned revenue flags) at the fact level 
    # to enforce consistency across all BI dashboards and alerting systems.
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