
# Databricks notebook source
# Global Mart: Retail Sales Anomaly Detection

from pyspark.sql import functions as F

SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA}")

# Load cleaned order items.
items = spark.table(f"{SILVER_SCHEMA}.order_items")

# Aggregate sales by product.
product_metrics = (
    items
    .groupBy("product_id")
    .agg(
        F.count("*").alias("item_count"),
        F.round(F.avg("price"), 2).alias("average_price"),
        F.round(F.stddev("price"), 2).alias("price_stddev"),
        F.round(F.sum("price"), 2).alias("total_sales")
    )
)

# Calculate the approximate interquartile range.
quartiles = items.approxQuantile("price", [0.25, 0.75], 0.01)

if len(quartiles) == 2:
    q1, q3 = quartiles
    iqr = q3 - q1
    lower_bound = max(0.0, q1 - 1.5 * iqr)
    upper_bound = q3 + 1.5 * iqr

    anomalies = (
        items
        .filter(
            (F.col("price") < lower_bound) |
            (F.col("price") > upper_bound)
        )
        .withColumn(
            "anomaly_reason",
            F.when(
                F.col("price") > upper_bound,
                "Unusually high price"
            ).otherwise("Unusually low price")
        )
    )

    (
        anomalies.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{GOLD_SCHEMA}.price_anomalies")
    )

    print(f"Lower price bound: {lower_bound:.2f}")
    print(f"Upper price bound: {upper_bound:.2f}")
    display(anomalies)

else:
    print("Not enough valid price data to calculate anomaly bounds.")

display(product_metrics.orderBy(F.desc("total_sales")).limit(20))

print("Anomaly detection completed.")
