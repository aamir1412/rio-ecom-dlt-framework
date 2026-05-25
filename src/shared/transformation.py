# src/shared/transformation.py

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

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