import sys
import argparse
from pyspark.sql import SparkSession

def main():
    # 1. Initialize parsing engine for incoming task parameters
    parser = argparse.ArgumentParser(description="Unity Catalog Structural Bootstrap")
    parser.add_argument("--catalog_name", required=True)
    parser.add_argument("--landing_loc", required=True)
    parser.add_argument("--checkpoint_loc", required=True)
    parser.add_argument("--managed_loc", required=True)
    args = parser.parse_args()

    # 2. Extract targets safely into local variables
    catalog = args.catalog_name
    landing_loc = args.landing_loc
    checkpoint_loc = args.checkpoint_loc
    managed_loc = args.managed_loc

    # 3. Instantiate SparkSession context
    spark = SparkSession.builder.getOrCreate()
    
    print(f"Initializing Isolated DDL execution for Catalog: {catalog}")
    
    # 4. Create Catalog bound to dedicated ADLS Gen2 Managed Storage
    # spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog} MANAGED LOCATION '{managed_loc}'")     
    
    # 5. Build Schemas
    schemas = ["raw", "bronze", "silver", "gold"]
    for schema in schemas:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
        print(f"Validated Schema: {catalog}.{schema}")

    # 6. Build External Volumes inside the Raw Layer with strict subdirectory isolation
    print(f"Mounting External Volume for Landing Zone to: {landing_loc}")
    spark.sql(f"CREATE EXTERNAL VOLUME IF NOT EXISTS {catalog}.raw.vol_landing_zone LOCATION '{landing_loc}'")
    
    print(f"Mounting External Volume for Checkpoints to: {checkpoint_loc}")
    spark.sql(f"CREATE EXTERNAL VOLUME IF NOT EXISTS {catalog}.raw.schema_checkpoints LOCATION '{checkpoint_loc}'")
        
    print("Database infrastructure initialization complete.")

if __name__ == "__main__":
    main()