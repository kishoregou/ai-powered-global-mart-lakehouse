# Databricks notebook source
# AI-Powered Global Mart Retail Data Engineering Lakehouse
# Bronze Layer: Raw data ingestion into Delta tables

from pyspark.sql import functions as F
from pyspark.sql.types import *

# Update these paths to match your Databricks Volume.
SOURCE_PATH = "/Volumes/<catalog>/<schema>/<volume>/olist/"
BRONZE_SCHEMA = "bronze"

# Olist source CSV files and corresponding table names.
SOURCE_FILES = {
    "olist_customers_dataset.csv": "customers",
    "olist_orders_dataset.csv": "orders",
    "olist_order_items_dataset.csv": "order_items",
    "olist_order_payments_dataset.csv": "order_payments",
    "olist_order_reviews_dataset.csv": "order_reviews",
    "olist_products_dataset.csv": "products",
    "olist_sellers_dataset.csv": "sellers",
    "olist_geolocation_dataset.csv": "geolocation",
    "product_category_name_translation.csv": "category_translation",
}

# Create the Bronze schema in the current catalog.
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE_SCHEMA}")

# Ingest each CSV into a Bronze Delta table.
for filename, table_name in SOURCE_FILES.items():
    file_path = f"{SOURCE_PATH}{filename}"

    try:
        df = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(file_path)
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_file", F.lit(filename))
        )

        row_count = df.count()

        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(f"{BRONZE_SCHEMA}.{table_name}")
        )

        print(f"SUCCESS: {table_name} | Rows: {row_count}")

    except Exception as exc:
        print(f"FAILED: {table_name} | {exc}")
        raise

print("Bronze ingestion completed.")
