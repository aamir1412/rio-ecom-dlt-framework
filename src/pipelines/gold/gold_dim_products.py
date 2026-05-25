"""
Gold Domain: Products Dimension
Materialized dimension table supporting localized and translated merchandising performance metrics.
"""

import dlt
from pyspark.sql.functions import col, when, lit
from src.shared.spark_io import read_published_silver


@dlt.table(
    name="gold_dim_products",
    comment="Materialized Product Dimension with late-bound English translations.",
    table_properties={
        "quality": "gold",
        # Optimize clustering for rapid categorical aggregations and product lookups
        "pipelines.autoOptimize.zOrderCols": "product_category_name, product_category_name_english"
    }
)
def create_gold_dim_products():
    
    # Ingest the published product table.
    df_products = read_published_silver("silver_products")
    
    # Execute dimensional cleaning and apply business logic classifications
    return (
        df_products
        # Business Logic Phase: Handle missing translation gaps gracefully
        .withColumn(
            "product_category_name_english",
            when(col("product_category_name_english").isNull(), lit("unknown_or_untranslated"))
            .otherwise(col("product_category_name_english"))
        )
        # Structural Classification: Segment items based on weight for freight auditing
        .withColumn(
            "weight_class",
            when(col("product_weight_g") < 1000, lit("Lightweight (<1kg)"))
            .when((col("product_weight_g") >= 1000) & (col("product_weight_g") < 5000), lit("Standard (1-5kg)"))
            .otherwise(lit("Heavyweight (>5kg)"))
        )
    )