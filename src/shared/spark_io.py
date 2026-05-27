# src/shared/spark_io.py

import dlt
from pyspark.sql import SparkSession, DataFrame

# 1. Streaming Bronze Ingestion Utility
# Resolves the current execution catalog dynamically to stream data from the Bronze layer.
# Preserves decoupled state tracking for continuous processing.
def read_bronze_stream(table_name: str) -> DataFrame:
    """
    Standardizes environment-aware Bronze layer reads.
    Guarantees CI/CD promotion safety across DEV/PROD catalogs.
    """
    spark = SparkSession.builder.getOrCreate()    
    catalog = spark.conf.get("fw.catalog_name")
        
    return spark.readStream.table(f"`{catalog}`.bronze.{table_name}")
    

# 2. Silver Cross-Pipeline Accessor
# Reads fully materialized Silver tables as a static DataFrame. Breaks explicit 
# DLT DAG dependencies to isolate pipeline orchestration boundaries.
def read_published_silver(table_name: str) -> DataFrame:
    """
    Cross-pipeline reader for materialized Unity Catalog tables.
    Bypasses the internal DLT DAG compiler to enforce Gold layer scheduling independence.
    """
    spark = SparkSession.builder.getOrCreate()
    
    catalog = spark.conf.get("fw.catalog_name")
    return spark.table(f"`{catalog}`.silver.{table_name}")
    

# 3. Gold Cross-Pipeline Accessor
# Exposes independent Gold analytical tables as static references for the presentation 
# layer without bundling pipelines into a single monolithic execution DAG.
def read_published_gold(table_name: str) -> DataFrame:
    """
    Cross-pipeline reader for materialized Gold Unity Catalog tables.
    Bypasses the internal DLT DAG compiler to enforce Presentation layer decoupling.
    """    
    
    spark = SparkSession.builder.getOrCreate() 

    catalog = spark.conf.get("fw.catalog_name")
    return spark.table(f"`{catalog}`.gold.{table_name}")