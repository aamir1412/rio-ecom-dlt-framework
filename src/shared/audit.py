from pyspark.sql.functions import current_timestamp, col, lit, sha2, concat_ws
from pyspark.sql import DataFrame

def add_bronze_metadata(df: DataFrame, pipeline_id: str) -> DataFrame:
    base_cols = [c for c in df.columns if not c.startswith("_")]
    return df.withColumn("_ingested_at", current_timestamp()) \
             .withColumn("_source_file", col("_metadata.file_path")) \
             .withColumn("_pipeline_id", lit(pipeline_id)) \
             .withColumn("_row_hash", sha2(concat_ws("||", *[col(c) for c in base_cols]), 256))