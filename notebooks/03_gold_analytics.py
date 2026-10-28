
# Databricks notebook source
# Gold Layer: Business-ready retail analytics

from pyspark.sql import functions as F

SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA}")

# Read cleaned Silver tables.
orders = spark.table(f"{SILVER_SCHEMA}.orders")
items = spark.table(f"{SILVER_SCHEMA}.order_items")

# Gold table 1: Sales and order metrics by month.
monthly_sales = (
    items
    .join(orders, on="order_id", how="inner")
    .filter(F.col("order_purchase_timestamp").isNotNull())
    .withColumn(
        "order_month",
        F.date_format("order_purchase_timestamp", "yyyy-MM")
    )
    .groupBy("order_month")
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.round(F.sum("price"), 2).alias("total_product_sales"),
        F.round(F.sum("freight_value"), 2).alias("total_freight")
    )
    .orderBy("order_month")
)

(
    monthly_sales.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD_SCHEMA}.monthly_sales")
)

# Gold table 2: Product-level performance.
product_sales = (
    items
    .groupBy("product_id")
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("order_item_id").alias("total_items"),
        F.round(F.sum("price"), 2).alias("total_sales"),
        F.round(F.avg("price"), 2).alias("average_price")
    )
    .orderBy(F.desc("total_sales"))
)

(
    product_sales.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD_SCHEMA}.product_sales")
)

# Display results in Databricks.
display(monthly_sales)
display(product_sales.limit(20))

print("Gold analytics tables created successfully.")
