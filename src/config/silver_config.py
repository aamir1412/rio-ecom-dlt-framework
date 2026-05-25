# src/config/silver_config.py

from pyspark.sql.functions import col, lpad, initcap, upper, broadcast
import dlt

# ==========================================
# CUSTOM TRANSFORMATION ESCAPE HATCHES
# ==========================================
def transform_products(df_raw, source_catalog):
    """Handles complex DAG enrichment for the products domain."""
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
        df_raw
        .withColumns(transformation_rules)
        .drop("product_name_lenght", "product_description_lenght")
    )
    
    return df_products.join(broadcast(df_translations), on="product_category_name", how="left")

# ==========================================
# SILVER METADATA REGISTRY
# ==========================================
SILVER_CONFIG = {
    "customers": {
        "scd_type": 2,
        "keys": ["customer_id"],
        "history_cols": ["customer_zip_code_prefix", "customer_city", "customer_state"],
        "expect_or_drop": {
            "valid_pk": "customer_id IS NOT NULL",
            "valid_state_length": "length(customer_state) = 2",
            "valid_zip_numeric": "customer_zip_code_prefix RLIKE '^[0-9]+$'"
        },
        "transform_rules": {
            "customer_zip_code_prefix": lpad(col("customer_zip_code_prefix"), 5, "0"),
            "customer_city": initcap(col("customer_city")),
            "customer_state": upper(col("customer_state"))
        }
    },
    "sellers": {
        "scd_type": 1,
        "keys": ["seller_id"],
        "expect_or_drop": {
            "valid_pk": "seller_id IS NOT NULL",
            "valid_state_length": "length(seller_state) = 2",
            "valid_zip_numeric": "seller_zip_code_prefix RLIKE '^[0-9]+$'"
        },
        "transform_rules": {
            "seller_zip_code_prefix": lpad(col("seller_zip_code_prefix"), 5, "0"),
            "seller_city": initcap(col("seller_city")),
            "seller_state": upper(col("seller_state"))
        }
    },
    "products": {
        "scd_type": 1,
        "keys": ["product_id"],
        "expect_or_drop": {
            "valid_pk": "product_id IS NOT NULL",
            "valid_weight": "product_weight_g >= 0"
        },
        # Maps to the custom function defined above instead of a static dictionary
        "transform_fn": transform_products
    },
    "orders": {
        "scd_type": 1,
        "keys": ["order_id"],
        "expect_or_drop": {
            "valid_pk": "order_id IS NOT NULL",
            "valid_status": "order_status IN ('delivered', 'shipped', 'canceled', 'invoiced', 'processing', 'unavailable', 'approved', 'created')"
        },
        "transform_rules": {
            t_col: col(t_col).cast("timestamp") for t_col in [
                "order_purchase_timestamp", "order_approved_at", 
                "order_delivered_carrier_date", "order_delivered_customer_date", 
                "order_estimated_delivery_date"
            ]
        }
    },
    "order_items": {
        "scd_type": 1,
        "keys": ["order_id", "order_item_id"],
        "expect_or_drop": {
            "valid_pk": "order_id IS NOT NULL AND order_item_id IS NOT NULL",
            "positive_price": "price >= 0",
            "positive_freight": "freight_value >= 0"
        },
        "transform_rules": {
            "price": col("price").cast("double"),
            "freight_value": col("freight_value").cast("double"),
            "shipping_limit_date": col("shipping_limit_date").cast("timestamp"),
            "order_item_id": col("order_item_id").cast("int")
        }
    },
    "order_payments": {
        "scd_type": 1,
        "keys": ["order_id", "payment_sequential"],
        "expect": {
            "valid_composite_pk": "order_id IS NOT NULL AND payment_sequential IS NOT NULL",
            "positive_value": "payment_value > 0"
        },
        "transform_rules": {
            "payment_value": col("payment_value").cast("double"),
            "payment_installments": col("payment_installments").cast("int"),
            "payment_sequential": col("payment_sequential").cast("int")
        }
    }
}