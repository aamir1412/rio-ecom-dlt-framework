# src/config/bronze_config.py

INGESTION_CONFIG = {
    "customers": {
        "schema_module": "src.domains.customers.schemas",
        "schema_obj": "customer_bronze_schema",
        "path": "customers"
    },
    "order_items": {
        "schema_module": "src.domains.orders.schemas",
        "schema_obj": "order_items_bronze_schema",
        "path": "order_items"
    },
    "order_payments": {
        "schema_module": "src.domains.orders.schemas",
        "schema_obj": "order_payments_bronze_schema",
        "path": "order_payments"
    },
    "orders": {
        "schema_module": "src.domains.orders.schemas",
        "schema_obj": "orders_bronze_schema",
        "path": "orders"
    },
    "product_translation": {
        "schema_module": "src.domains.products.schemas",
        "schema_obj": "product_translation_bronze_schema",
        "path": "product_translation"
    },
    "products": {
        "schema_module": "src.domains.products.schemas",
        "schema_obj": "products_bronze_schema",
        "path": "products",
        "load_type": "streaming"
    },
    "sellers": {
        "schema_module": "src.domains.sellers.schemas",
        "schema_obj": "sellers_bronze_schema",
        "path": "sellers",
        "load_type": "streaming"
    }
}