# src/domains/products/schemas.py
from pyspark.sql.types import StructType, StructField, StringType

# 1. Product Translation Schema
product_translation_bronze_schema = StructType([
    StructField("product_category_name", StringType(), True),
    StructField("product_category_name_english", StringType(), True),
    StructField("_rescued_data", StringType(), True)
]) 

# 2. Products Base Schema  
products_bronze_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_category_name", StringType(), True),
    StructField("product_name_lenght", StringType(), True),
    StructField("product_description_lenght", StringType(), True),
    StructField("product_photos_qty", StringType(), True),
    StructField("product_weight_g", StringType(), True),
    StructField("product_length_cm", StringType(), True),
    StructField("product_height_cm", StringType(), True),
    StructField("product_width_cm", StringType(), True),
    StructField("_rescued_data", StringType(), True)
])