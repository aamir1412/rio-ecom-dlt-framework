# src/domains/products/schemas.py
from pyspark.sql.types import StructType, StructField, StringType

product_translation_bronze_schema = StructType([
    StructField("product_category_name", StringType(), True),
    StructField("product_category_name_english", StringType(), True),
    StructField("_rescued_data", StringType(), True)
])

products_bronze_schema = """
    product_id STRING,
    product_category_name STRING,
    product_name_lenght STRING,
    product_description_lenght STRING,
    product_photos_qty STRING,
    product_weight_g STRING,
    product_length_cm STRING,
    product_height_cm STRING,
    product_width_cm STRING,
    _rescued_data STRING
"""