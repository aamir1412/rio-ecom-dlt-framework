# src/pipelines/02_silver.py

import dlt
from pyspark.sql.functions import col, lpad, initcap, upper
from src.shared.audit import apply_silver_metadata

@dlt.view(
    name="silver_customers_stg",
    comment="Transient view staging cleaned customer data."
)
@dlt.expect_or_drop("valid_pk", "customer_id IS NOT NULL")
@dlt.expect_all({
    "valid_state_length": "length(customer_state) = 2",
    "valid_zip_numeric": "customer_zip_code_prefix RLIKE '^[0-9]+$'"
})
def create_silver_customers_stg():
    
    transformation_rules = {
        "customer_zip_code_prefix": lpad(col("customer_zip_code_prefix"), 5, "0"),
        "customer_city": initcap(col("customer_city")),
        "customer_state": upper(col("customer_state"))
    }
        
    df_normalized = (
        dlt.read_stream("cat_ecom_dev.bronze.bronze_customers")
        .withColumns(transformation_rules)
    )
    
    return apply_silver_metadata(df_normalized)


dlt.create_streaming_table(
    name="silver_customers",
    comment="SCD Type 1 Customer Master Dimension.",
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true" 
    }
)

# EXECUTION ENGINE: DLT AUTO CDC (SCD Type 2)
dlt.apply_changes(
    target="silver_customers",
    source="silver_customers_stg",
    keys=["customer_id"],
    # Deterministic sequencing via hardware-level file timestamps
    sequence_by=col("_bronze_source_file_modified"), 
    stored_as_scd_type=2,
    # MANDATORY: Explicitly define which columns trigger a historical version
    track_history_column_list=[
        "customer_zip_code_prefix", 
        "customer_city", 
        "customer_state"
    ]
)


import dlt
from pyspark.sql.functions import col, lpad, initcap, upper
from src.shared.audit import apply_silver_metadata


# SELLERS STAGING VIEW
@dlt.view(
    name="silver_sellers_stg",
    comment="Transient view staging cleaned seller data."
)
@dlt.expect_or_drop("valid_pk", "seller_id IS NOT NULL")
@dlt.expect_all({
    "valid_state_length": "length(seller_state) = 2",
    "valid_zip_numeric": "seller_zip_code_prefix RLIKE '^[0-9]+$'"
})
def create_silver_sellers_stg():
    
    transformation_rules = {
        "seller_zip_code_prefix": lpad(col("seller_zip_code_prefix"), 5, "0"),
        "seller_city": initcap(col("seller_city")),
        "seller_state": upper(col("seller_state"))
    }
    
    # Resolves dependency dynamically within the pipeline graph
    df_normalized = (
        dlt.read_stream("cat_ecom_dev.bronze.bronze_sellers")
        .withColumns(transformation_rules)
    )
    
    return apply_silver_metadata(df_normalized)


# SELLERS TARGET TABLE (CDF ENABLED)
dlt.create_streaming_table(
    name="silver_sellers",
    comment="SCD Type 1 Seller Master Dimension (Static Reference).",
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true" 
    }
)


# EXECUTION ENGINE: DLT AUTO CDC
dlt.apply_changes(
    target="silver_sellers",
    source="silver_sellers_stg",
    keys=["seller_id"],
    sequence_by=col("_bronze_source_file_modified"), 
    stored_as_scd_type=1
)


import dlt
from pyspark.sql.functions import col, broadcast
from src.shared.audit import apply_silver_metadata

# 1. PRODUCTS STAGING VIEW & ENRICHMENT
@dlt.view(
    name="silver_products_stg",
    comment="Transient view staging cleaned and translated product data."
)
@dlt.expect_or_drop("valid_pk", "product_id IS NOT NULL")
@dlt.expect_or_drop("valid_weight", "product_weight_g >= 0")
def create_silver_products_stg():
    
    # 1. Isolate the translation lookup to prevent metadata column collisions
    # Using dlt.read() instead of read_stream() forces a static broadcast lookup
    df_translations = dlt.read("cat_ecom_dev.bronze.bronze_product_translation").select(
        "product_category_name",
        "product_category_name_english"
    )
    
    # 2. Extract and cast primary streaming payload
    df_products = (
        dlt.read_stream("cat_ecom_dev.bronze.bronze_products")
        .withColumnRenamed("product_name_lenght", "product_name_length")
        .withColumnRenamed("product_description_lenght", "product_description_length")
        .withColumn("product_weight_g", col("product_weight_g").cast("double"))
        .withColumn("product_length_cm", col("product_length_cm").cast("double"))
        .withColumn("product_height_cm", col("product_height_cm").cast("double"))
        .withColumn("product_width_cm", col("product_width_cm").cast("double"))
    )
    
    # 3. Broadcast Left Join (Micro-batch to Static)
    df_enriched = df_products.join(
        broadcast(df_translations),
        on="product_category_name",
        how="left"
    )
    
    return apply_silver_metadata(df_enriched)

# PRODUCTS TARGET TABLE (CDF ENABLED)
dlt.create_streaming_table(
    name="silver_products",
    comment="SCD Type 1 Product Master Dimension.",
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true" 
    }
)

# EXECUTION ENGINE: DLT AUTO CDC
dlt.apply_changes(
    target="silver_products",
    source="silver_products_stg",
    keys=["product_id"],
    sequence_by=col("_bronze_source_file_modified"), 
    stored_as_scd_type=1
)

# Appended to src/pipelines/02_silver.py (or isolated in 02_silver_orders.py)

import dlt
from pyspark.sql.functions import col
from src.shared.audit import apply_silver_metadata


# 1. ORDERS STAGING VIEW
@dlt.view(
    name="silver_orders_stg",
    comment="Transient view staging cleaned order data with casted timestamps."
)
@dlt.expect_or_drop("valid_pk", "order_id IS NOT NULL")
@dlt.expect_or_drop(
    "valid_status", 
    "order_status IN ('delivered', 'shipped', 'canceled', 'invoiced', 'processing', 'unavailable', 'approved', 'created')"
)
def create_silver_orders_stg():
    
    # 1. Explicitly read from the physical Unity Catalog namespace
    df_raw = dlt.read_stream("cat_ecom_dev.bronze.bronze_orders")
    
    # 2. Iterative column casting for temporal fields
    timestamp_columns = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date"
    ]
    
    df_casted = df_raw
    for t_col in timestamp_columns:
        df_casted = df_casted.withColumn(t_col, col(t_col).cast("timestamp"))
        
    return apply_silver_metadata(df_casted)

# 2. ORDERS TARGET TABLE (CDF ENABLED)
dlt.create_streaming_table(
    name="silver_orders",
    comment="SCD Type 1 Order Master Fact.",
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true" 
    }
)

# 3. EXECUTION ENGINE: DLT AUTO CDC
dlt.apply_changes(
    target="silver_orders",
    source="silver_orders_stg",
    keys=["order_id"],
    sequence_by=col("_bronze_source_file_modified"), 
    stored_as_scd_type=1
)


# Appended to src/pipelines/02_silver.py (or isolated in 02_silver_order_items.py)

import dlt
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from src.shared.audit import apply_silver_metadata

# Dynamic Environment Configuration Resolution
spark = SparkSession.builder.getOrCreate()
source_catalog = spark.conf.get("source_catalog", "cat_ecom_dev")

# 1. ORDER ITEMS STAGING VIEW
@dlt.view(
    name="silver_order_items_stg",
    comment="Transient view staging cleaned order items data."
)
@dlt.expect_or_drop("valid_pk", "order_id IS NOT NULL")
@dlt.expect_or_drop("positive_price", "price >= 0")
@dlt.expect_or_drop("positive_freight", "freight_value >= 0")
def create_silver_order_items_stg():
    
    # Decoupled environment-aware reader
    df_raw = dlt.read_stream(f"{source_catalog}.bronze.bronze_order_items")
    
    # Apply structural casting
    df_casted = (
        df_raw
        .withColumn("price", col("price").cast("double"))
        .withColumn("freight_value", col("freight_value").cast("double"))
        .withColumn("shipping_limit_date", col("shipping_limit_date").cast("timestamp"))
        .withColumn("order_item_id", col("order_item_id").cast("int"))
    )
        
    return apply_silver_metadata(df_casted)

# 2. ORDER ITEMS TARGET TABLE (CDF ENABLED)
dlt.create_streaming_table(
    name="silver_order_items",
    comment="SCD Type 1 Order Items Fact Dimension.",
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true" 
    }
)

# ==========================================
# 3. EXECUTION ENGINE: DLT AUTO CDC
# ==========================================
dlt.apply_changes(
    target="silver_order_items",
    source="silver_order_items_stg",
    # CRITICAL: Composite primary key definition
    keys=["order_id", "order_item_id"],
    sequence_by=col("_bronze_source_file_modified"), 
    stored_as_scd_type=1
)