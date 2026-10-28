# Global Mart Lakehouse Architecture

## 1. Overview

The AI-Powered Global Mart Retail Data Engineering Lakehouse processes raw e-commerce data into analytics-ready datasets using Databricks, PySpark, SQL, and Delta Lake.

## 2. Architecture Flow

```text
Olist CSV Files
      |
      v
Bronze Layer
Raw data ingestion into Delta tables
      |
      v
Silver Layer
Cleaning, deduplication, standardization
      |
      v
Gold Layer
Business metrics and analytical tables
      |
      v
Data Quality and Anomaly Detection
      |
      v
Business Analytics
```

## 3. Bronze Layer

- Reads source CSV files.
- Adds ingestion timestamps and source file metadata.
- Stores raw data in Delta tables.

## 4. Silver Layer

- Removes duplicate records.
- Filters invalid records.
- Standardizes date and numeric columns.
- Produces cleaned tables for downstream analysis.

## 5. Gold Layer

- Calculates monthly sales metrics.
- Aggregates product sales.
- Creates business-ready analytical tables.

## 6. Data Quality and Anomaly Detection

- Checks duplicate order IDs.
- Identifies missing order identifiers.
- Flags invalid prices.
- Uses the interquartile range (IQR) method to detect unusual prices.

## 7. Technology Stack

- Databricks
- PySpark
- SQL
- Delta Lake
- Python
- Git and GitHub

## 8. Execution Requirements

Configure the Databricks catalog, schema, source Volume paths, and permissions before running the notebooks. Execute the Bronze layer first, followed by Silver, Gold, and the quality checks.

**Status:** Development in progress. End-to-end execution and validation are still required.
