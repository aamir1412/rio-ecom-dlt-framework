# src/shared/audit.py

from pyspark.sql.functions import current_timestamp, col, lit, sha2, concat_ws
from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException

# 1. Bronze Metadata Enrichment
# Appends ingestion lineage fields to the incoming raw DataFrame. Captures Auto Loader 
# file metadata, assigns the runtime pipeline ID, and builds a unique SHA-256 row signature.
def add_bronze_metadata(df: DataFrame, pipeline_id: str) -> DataFrame:
    """
    Injects foundational structural lineage, including hardware-level file timestamps
    required for deterministic CDC sequencing downstream.
    """
    base_cols = [c for c in df.columns if not c.startswith("_")]
    
    # Extract hardware-level timestamp for deterministic sequencing
    try:    
        df = (df.withColumn("_source_file", col("_metadata.file_path"))
                .withColumn("_source_file_modified", col("_metadata.file_modification_time")))
    except AnalysisException:
    # Fallback triggers ONLY if _metadata physically does not exist     
        df = (df.withColumn("_source_file", lit("unknown_source"))
                .withColumn("_source_file_modified", current_timestamp()))
    
    return (
        df
        .withColumn("_ingested_at", current_timestamp()) 
        .withColumn("_pipeline_id", lit(pipeline_id)) 
        .withColumn("_row_hash", sha2(concat_ws("||", *[col(c) for c in base_cols]), 256))
    )


# 2. Silver Metadata Normalization
# Re-namespaces existing Bronze metadata fields to prevent naming collisions, purges 
# transient staging attributes, and records the Silver layer system modification timestamp.
def apply_silver_metadata(df: DataFrame) -> DataFrame:
    """
    Namespaces Bronze metadata for historical tracking and injects the Silver update timestamp.
    """
    existing_cols = df.columns

    # Namespace lineage to preserve history while avoiding column ambiguity
    renames = {
        "_row_hash": "_bronze_row_hash",
        "_source_file": "_bronze_source_file",
        "_ingested_at": "_bronze_ingested_at",
        "_source_file_modified": "_bronze_source_file_modified"
    }
    
    for old_col, new_col in renames.items():
        if old_col in existing_cols:
            df = df.withColumnRenamed(old_col, new_col)

    # Purge operational artifacts that hold no value in the dimensional model
    transient_cols = [
        "_rescued_data",
        "_pipeline_id"
    ]
    cols_to_drop = [c for c in transient_cols if c in existing_cols]

    return (
        df
        .withColumn("_updated_at", current_timestamp())
        .drop(*cols_to_drop)
    )