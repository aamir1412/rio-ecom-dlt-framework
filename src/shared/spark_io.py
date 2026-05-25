# src/shared/spark_io.py

import dlt
from pyspark.sql import SparkSession, DataFrame

def read_bronze_stream(table_name: str) -> DataFrame:
    """
    Standardizes environment-aware Bronze layer reads.
    Guarantees CI/CD promotion safety across DEV/PROD catalogs.
    """
    spark = SparkSession.builder.getOrCreate()
    source_catalog = spark.conf.get("source_catalog", "cat_ecom_dev")
    
    return dlt.read_stream(f"{source_catalog}.bronze.{table_name}")


def read_published_silver(table_name: str) -> DataFrame:
    """
    Cross-pipeline reader for materialized Unity Catalog tables.
    Bypasses the internal DLT DAG compiler to enforce Gold layer scheduling independence.
    """
    spark = SparkSession.builder.getOrCreate()
    source_catalog = spark.conf.get("source_catalog", "cat_ecom_dev")
    
    return spark.table(f"{source_catalog}.silver.{table_name}")