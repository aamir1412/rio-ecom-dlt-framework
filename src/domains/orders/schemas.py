# src/domains/order_items/schemas.py
from pyspark.sql.types import StructType, StructField, StringType

order_items_bronze_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("order_item_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("seller_id", StringType(), True),
    StructField("shipping_limit_date", StringType(), True),
    StructField("price", StringType(), True),
    StructField("freight_value", StringType(), True),
    StructField("_rescued_data", StringType(), True)
])

order_payments_bronze_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("payment_sequential", StringType(), True),
    StructField("payment_type", StringType(), True),
    StructField("payment_installments", StringType(), True),
    StructField("payment_value", StringType(), True),
    StructField("_rescued_data", StringType(), True)
])

orders_bronze_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("order_purchase_timestamp", StringType(), True),
    StructField("order_approved_at", StringType(), True),
    StructField("order_delivered_carrier_date", StringType(), True),
    StructField("order_delivered_customer_date", StringType(), True),
    StructField("order_estimated_delivery_date", StringType(), True),
    StructField("_rescued_data", StringType(), True)
])