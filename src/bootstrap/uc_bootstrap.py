"""
Tier 2 Initialization Script
Executes inside the pre-existing catalog shell to map out operational schemas and external volumes.
"""
import os
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Retrieve dynamic parameters from the DABs cluster context
catalog = os.environ.get("CATALOG_NAME")
landing_loc = os.environ.get("LANDING_LOC")
checkpoint_loc = os.environ.get("CHECKPOINT_LOC")

print(f"Executing Tier 2 structural build for Catalog: {catalog}")

# 1. Build Schemas
schemas = ["raw", "bronze", "silver", "gold"]
for schema in schemas:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    print(f"Validated Schema: {catalog}.{schema}")

# 2. Build External Volumes inside the Raw Layer
spark.sql(f"CREATE EXTERNAL VOLUME IF NOT EXISTS {catalog}.raw.vol_landing_zone LOCATION '{landing_loc}'")
spark.sql(f"CREATE EXTERNAL VOLUME IF NOT EXISTS {catalog}.raw.schema_checkpoints LOCATION '{checkpoint_loc}'")
