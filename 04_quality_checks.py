
# Databricks notebook source
# Data Quality Checks for Global Mart Lakehouse

from pyspark.sql import functions as F

SILVER_SCHEMA = "silver"

orders = spark.table(f"{SILVER_SCHEMA}.orders")
items = spark.table(f"{SILVER_SCHEMA}.order_items")

# Check duplicate order IDs.
duplicate_orders = (
    orders.groupBy("order_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

# Check missing order IDs.
missing_order_ids = orders.filter(
    F.col("order_id").isNull()
).count()

# Check invalid product prices.
invalid_prices = items.filter(
    F.col("price").isNull() | (F.col("price") < 0)
).count()

# Build a simple quality report.
quality_report = spark.createDataFrame(
    [
        ("duplicate_orders", duplicate_orders),
        ("missing_order_ids", missing_order_ids),
        ("invalid_prices", invalid_prices),
    ],
    ["check_name", "issue_count"]
)

display(quality_report)

print("Data quality checks completed.")
