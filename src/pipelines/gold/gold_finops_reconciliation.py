# src/pipelines/gold/gold_finops_reconciliation.py

import dlt
from pyspark.sql.functions import col, sum, round, when, lit, coalesce

@dlt.table(
    name="gold_fact_financial_reconciliation",
    comment="Materialized View for FinOps ledger balancing. Denormalized to the order grain.",
    table_properties={
        "quality": "gold",
        # Optimize storage layout for the most frequent BI filtering pattern
        "pipelines.autoOptimize.zOrderCols": "order_purchase_timestamp, order_status"
    }
)
def create_gold_fact_financial_reconciliation():
    
    # 1. Read static snapshots of the Silver layer
    df_orders = dlt.read("silver_orders")
    df_items = dlt.read("silver_order_items")
    df_payments = dlt.read("silver_order_payments")
    
    # 2. ISOLATION: Pre-aggregate physical product value to the order grain
    df_items_agg = (
        df_items.groupBy("order_id")
        .agg(
            sum(col("price") + col("freight_value")).alias("total_order_value")
        )
    )
    
    # 3. ISOLATION: Pre-aggregate financial ledger collections to the order grain
    df_payments_agg = (
        df_payments.groupBy("order_id")
        .agg(
            # Using standard double sum; assume Silver layer utilizes DECIMAL(10,2)
            sum("payment_value").alias("total_payment_collected"),
            sum("payment_installments").alias("total_installments")
        )
    )
    
    # 4. DENORMALIZATION: Build the unified analytical Star schema fact
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
    
    # 5. BUSINESS LOGIC: Calculate FinOps KPIs and risk flags
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