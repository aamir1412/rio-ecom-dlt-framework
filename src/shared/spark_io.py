# src/shared/spark_io.py

import dlt
from pyspark.sql import SparkSession, DataFrame

def read_bronze_stream(table_name: str) -> DataFrame:
    """
    Standardizes environment-aware Bronze layer reads.
    Guarantees CI/CD promotion safety across DEV/PROD catalogs.
    """
    spark = SparkSession.builder.getOrCreate()    
    catalog = spark.conf.get("fw.catalog_name")
        
    return spark.readStream.table(f"`{catalog}`.bronze.{table_name}")
    

def read_published_silver(table_name: str) -> DataFrame:
    """
    Cross-pipeline reader for materialized Unity Catalog tables.
    Bypasses the internal DLT DAG compiler to enforce Gold layer scheduling independence.
    """
    spark = SparkSession.builder.getOrCreate()
    
    catalog = spark.conf.get("fw.catalog_name")
    return spark.table(f"`{catalog}`.silver.{table_name}")
    

def read_published_gold(table_name: str) -> DataFrame:
    """
    Cross-pipeline reader for materialized Gold Unity Catalog tables.
    Bypasses the internal DLT DAG compiler to enforce Presentation layer decoupling.
    """    
    
    spark = SparkSession.builder.getOrCreate() 

    catalog = spark.conf.get("fw.catalog_name")
    return spark.table(f"`{catalog}`.gold.{table_name}")