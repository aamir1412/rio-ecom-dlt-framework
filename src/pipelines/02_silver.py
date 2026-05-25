# src/pipelines/02_silver.py

import dlt
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lpad, initcap, upper, broadcast
from src.shared.audit import apply_silver_metadata

# ==========================================
# 0. GLOBAL PIPELINE CONFIGURATION & UTILS
# ==========================================
spark = SparkSession.builder.getOrCreate()
source_catalog = spark.conf.get("source_catalog", "cat_ecom_dev")

def read_bronze_stream(table_name: str):
    """
    Modular utility to standardizes environment-aware Bronze layer reads.
    Guarantees CI/CD promotion safety across DEV/PROD catalogs.
    """
    return dlt.read_stream(f"{source_catalog}.bronze.{table_name}")


# ==========================================
# 1. CUSTOMERS DOMAIN (SCD TYPE 2)
# ==========================================
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
        read_bronze_stream("bronze_customers")
        .withColumns(transformation_rules)
    )
    return apply_silver_metadata(df_normalized)

dlt.create_streaming_table(
    name="silver_customers",
    comment="SCD Type 2 Customer Master Dimension.",
    table_properties={"quality": "silver", "delta.enableChangeDataFeed": "true"}
)

dlt.apply_changes(
    target="silver_customers",
    source="silver_customers_stg",
    keys=["customer_id"],
    sequence_by=col("_bronze_source_file_modified"), 
    stored_as_scd_type=2,
    track_history_column_list=["customer_zip_code_prefix", "customer_city", "customer_state"]
)


# ==========================================
# 2. SELLERS DOMAIN (SCD TYPE 1)
# ==========================================
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
    
    df_normalized = (
        read_bronze_stream("bronze_sellers")
        .withColumns(transformation_rules)
    )
    return apply_silver_metadata(df_normalized)

dlt.create_streaming_table(
    name="silver_sellers",
    comment="SCD Type 1 Seller Master Dimension (Static Reference).",
    table_properties={"quality": "silver", "delta.enableChangeDataFeed": "true"}
)

dlt.apply_changes(
    target="silver_sellers",
    source="silver_sellers_stg",
    keys=["seller_id"],
    sequence_by=col("_bronze_source_file_modified"), 
    stored_as_scd_type=1
)


# ==========================================
# 3. PRODUCTS DOMAIN (SCD TYPE 1)
# ==========================================
@dlt.view(
    name="silver_products_stg",
    comment="Transient view staging cleaned and translated product data."
)
@dlt.expect_or_drop("valid_pk", "product_id IS NOT NULL")
@dlt.expect_or_drop("valid_weight", "product_weight_g >= 0")
def create_silver_products_stg():
    df_translations = dlt.read(f"{source_catalog}.bronze.bronze_product_translation").select(
        "product_category_name",
        "product_category_name_english"
    )
    
    transformation_rules = {
        "product_name_length": col("product_name_lenght"),
        "product_description_length": col("product_description_lenght"),
        "product_weight_g": col("product_weight_g").cast("double"),
        "product_length_cm": col("product_length_cm").cast("double"),
        "product_height_cm": col("product_height_cm").cast("double"),
        "product_width_cm": col("product_width_cm").cast("double")
    }
    
    df_products = (
        read_bronze_stream("bronze_products")
        .withColumns(transformation_rules)
        .drop("product_name_lenght", "product_description_lenght")
    )
    
    df_enriched = df_products.join(
        broadcast(df_translations),
        on="product_category_name",
        how="left"
    )
    return apply_silver_metadata(df_enriched)

dlt.create_streaming_table(
    name="silver_products",
    comment="SCD Type 1 Product Master Dimension.",
    table_properties={"quality": "silver", "delta.enableChangeDataFeed": "true"}
)

dlt.apply_changes(
    target="silver_products",
    source="silver_products_stg",
    keys=["product_id"],
    sequence_by=col("_bronze_source_file_modified"), 
    stored_as_scd_type=1
)


# ==========================================
# 4. ORDERS DOMAIN (SCD TYPE 1)
# ==========================================
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
    timestamp_columns = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date"
    ]
    
    # Dynamic dictionary comprehension for type casting
    transformation_rules = {t_col: col(t_col).cast("timestamp") for t_col in timestamp_columns}
    
    df_casted = (
        read_bronze_stream("bronze_orders")
        .withColumns(transformation_rules)
    )
    return apply_silver_metadata(df_casted)

dlt.create_streaming_table(
    name="silver_orders",
    comment="SCD Type 1 Order Master Fact.",
    table_properties={"quality": "silver", "delta.enableChangeDataFeed": "true"}
)

dlt.apply_changes(
    target="silver_orders",
    source="silver_orders_stg",
    keys=["order_id"],
    sequence_by=col("_bronze_source_file_modified"), 
    stored_as_scd_type=1
)


# ==========================================
# 5. ORDER ITEMS DOMAIN (SCD TYPE 1)
# ==========================================
@dlt.view(
    name="silver_order_items_stg",
    comment="Transient view staging cleaned order items data."
)
@dlt.expect_or_drop("valid_pk", "order_id IS NOT NULL AND order_item_id IS NOT NULL")
@dlt.expect_or_drop("positive_price", "price >= 0")
@dlt.expect_or_drop("positive_freight", "freight_value >= 0")
def create_silver_order_items_stg():
    transformation_rules = {
        "price": col("price").cast("double"),
        "freight_value": col("freight_value").cast("double"),
        "shipping_limit_date": col("shipping_limit_date").cast("timestamp"),
        "order_item_id": col("order_item_id").cast("int")
    }
    
    df_casted = (
        read_bronze_stream("bronze_order_items")
        .withColumns(transformation_rules)
    )
    return apply_silver_metadata(df_casted)

dlt.create_streaming_table(
    name="silver_order_items",
    comment="SCD Type 1 Order Items Fact Dimension.",
    table_properties={"quality": "silver", "delta.enableChangeDataFeed": "true"}
)

dlt.apply_changes(
    target="silver_order_items",
    source="silver_order_items_stg",
    keys=["order_id", "order_item_id"],
    sequence_by=col("_bronze_source_file_modified"), 
    stored_as_scd_type=1
)


# ==========================================
# 6. ORDER PAYMENTS DOMAIN (SCD TYPE 1)
# ==========================================
@dlt.view(
    name="silver_order_payments_stg",
    comment="Transient view staging cleaned order payments data."
)
@dlt.expect("valid_composite_pk", "order_id IS NOT NULL AND payment_sequential IS NOT NULL")
@dlt.expect("positive_value", "payment_value > 0")
def create_silver_order_payments_stg():
    transformation_rules = {
        "payment_value": col("payment_value").cast("double"),
        "payment_installments": col("payment_installments").cast("int"),
        "payment_sequential": col("payment_sequential").cast("int")
    }
    
    df_casted = (
        read_bronze_stream("bronze_order_payments")
        .withColumns(transformation_rules)
    )
    return apply_silver_metadata(df_casted)

dlt.create_streaming_table(
    name="silver_order_payments",
    comment="SCD Type 1 Order Payments Fact Dimension.",
    table_properties={"quality": "silver", "delta.enableChangeDataFeed": "true"}
)

dlt.apply_changes(
    target="silver_order_payments",
    source="silver_order_payments_stg",
    keys=["order_id", "payment_sequential"],
    sequence_by=col("_bronze_source_file_modified"), 
    stored_as_scd_type=1
)