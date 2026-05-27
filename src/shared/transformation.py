# src/shared/transformation.py

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

# 1. Single-Pass Column Renaming
# Generates a dynamic array of select expressions to rename target columns. 
# Avoids repetitive, sequential .withColumnRenamed calls to minimize Spark query plan depth.
def rename_columns(df: DataFrame, column_mapping: dict) -> DataFrame:
    """
    Renames DataFrame columns via a dictionary mapping.
    Executes as a single-pass Catalyst alias projection for optimal performance.
    """
    select_expressions = []
    
    for current_col in df.columns:
        # If the column exists in our dictionary, apply the new alias
        if current_col in column_mapping:
            new_col_name = column_mapping[current_col]
            select_expressions.append(col(current_col).alias(new_col_name))
        
        # Otherwise, keep the column exactly as it is
        else:
            select_expressions.append(col(current_col))
            
    return df.select(*select_expressions)

# 2. Bulk Column Type Casting
# Leverages Spark's native .withColumns interface to execute multi-column data type 
# conversions within a single Spark logical plan projection.
def cast_columns(df: DataFrame, type_mapping: dict) -> DataFrame:
    """
    Applies bulk type casting via a dictionary mapping.
    Format: {"column_name": "target_type"}
    """
    cast_rules = {
        col_name: col(col_name).cast(target_type) 
        for col_name, target_type in type_mapping.items()
    }
    return df.withColumns(cast_rules)