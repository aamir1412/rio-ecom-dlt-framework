# src/domains/sellers/schemas.py

from pyspark.sql.types import StructType, StructField, StringType

# 1. Sellers Base Schema (Converted to StructType for Consistency)
sellers_bronze_schema = StructType([
    StructField("seller_id", StringType(), True),
    StructField("seller_zip_code_prefix", StringType(), True),
    StructField("seller_city", StringType(), True),
    StructField("seller_state", StringType(), True),
    StructField("_rescued_data", StringType(), True)
])