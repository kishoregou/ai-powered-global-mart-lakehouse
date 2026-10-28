
# Databricks notebook source
# AI-Powered Global Mart Lakehouse
# Machine Learning: Late Delivery Prediction

from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator
)

SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA}")

# Load historical order data.
orders = spark.table(f"{SILVER_SCHEMA}.orders")

# Create a training dataset using actual delivery outcomes.
data = (
    orders
    .filter(F.col("order_delivered_customer_date").isNotNull())
    .filter(F.col("order_estimated_delivery_date").isNotNull())
    .withColumn(
        "delivery_days",
        F.datediff(
            F.col("order_estimated_delivery_date"),
            F.col("order_purchase_timestamp")
        )
    )
    .withColumn(
        "label",
        F.when(
            F.col("order_delivered_customer_date") >
            F.col("order_estimated_delivery_date"),
            1.0
        ).otherwise(0.0)
    )
    .withColumn(
        "purchase_month",
        F.month("order_purchase_timestamp").cast("double")
    )
    .withColumn(
        "purchase_day_of_week",
        F.dayofweek("order_purchase_timestamp").cast("double")
    )
    .withColumn(
        "estimated_delivery_days",
        F.datediff(
            F.col("order_estimated_delivery_date"),
            F.col("order_purchase_timestamp")
        ).cast("double")
    )
    .filter(F.col("estimated_delivery_days") >= 0)
    .select(
        "purchase_month",
        "purchase_day_of_week",
        "estimated_delivery_days",
        "label"
    )
    .dropna()
)

# Check whether enough examples and both classes exist.
class_counts = {
    row["label"]: row["count"]
    for row in data.groupBy("label").count().collect()
}

if len(class_counts) < 2 or data.count() < 100:
    raise ValueError(
        "Insufficient data or only one class found. "
        "Check the Silver orders table before training."
    )

# Split records into training and test datasets.
train_data, test_data = data.randomSplit(
    [0.8, 0.2],
    seed=42
)

# Train a Random Forest classifier.
assembler = VectorAssembler(
    inputCols=[
        "purchase_month",
        "purchase_day_of_week",
        "estimated_delivery_days"
    ],
    outputCol="features"
)

classifier = RandomForestClassifier(
    featuresCol="features",
    labelCol="label",
    numTrees=50,
    seed=42
)

pipeline = Pipeline(stages=[assembler, classifier])

model = pipeline.fit(train_data)

# Evaluate predictions on held-out test data.
predictions = model.transform(test_data)

auc = BinaryClassificationEvaluator(
    labelCol="label",
    rawPredictionCol="rawPrediction",
    metricName="areaUnderROC"
).evaluate(predictions)

f1 = MulticlassClassificationEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="f1"
).evaluate(predictions)

print(f"Test ROC-AUC: {auc:.4f}")
print(f"Test F1 score: {f1:.4f}")

display(
    predictions.select(
        "label",
        "prediction",
        "probability"
    ).limit(20)
)

# Save predictions to a Gold Delta table.
(
    predictions
    .withColumn("prediction_generated_at", F.current_timestamp())
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD_SCHEMA}.delivery_predictions")
)

print("Delivery prediction model trained and predictions saved.")
