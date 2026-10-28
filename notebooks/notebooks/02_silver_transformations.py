
# Databricks notebook source
# Silver Layer: Clean and standardize retail data

from pyspark.sql import functions as F

BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_SCHEMA}")

# Read Bronze orders.
orders = spark.table(f"{BRONZE_SCHEMA}.orders")

# Clean orders and convert date columns.
silver_orders = (
    orders
    .dropDuplicates(["order_id"])
    .filter(F.col("order_id").isNotNull())
    .withColumn(
        "order_purchase_timestamp",
        F.to_timestamp("order_purchase_timestamp")
    )
    .withColumn(
        "order_approved_at",
        F.to_timestamp("order_approved_at")
    )
    .withColumn(
        "order_delivered_customer_date",
        F.to_timestamp("order_delivered_customer_date")
    )
    .withColumn(
        "order_estimated_delivery_date",
        F.to_timestamp("order_estimated_delivery_date")
    )
)

# Save cleaned orders to Silver Delta table.
(
    silver_orders.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{SILVER_SCHEMA}.orders")
)

print(f"Silver orders created: {silver_orders.count()} rows")

# Clean order items.
items = spark.table(f"{BRONZE_SCHEMA}.order_items")

silver_items = (
    items
    .dropDuplicates(["order_id", "order_item_id"])
    .filter(F.col("order_id").isNotNull())
    .filter(F.col("product_id").isNotNull())
    .withColumn("price", F.col("price").cast("double"))
    .withColumn("freight_value", F.col("freight_value").cast("double"))
    .filter(F.col("price") >= 0)
    .filter(F.col("freight_value") >= 0)
)

(
    silver_items.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{SILVER_SCHEMA}.order_items")
)

print(f"Silver order items created: {silver_items.count()} rows")
print("Silver transformations completed.")
