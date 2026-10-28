# AI-Powered Global Mart Retail Data Engineering Lakehouse

## Project Overview
An end-to-end retail data engineering project built using Databricks, PySpark, SQL, and Delta Lake. The project uses the Medallion Architecture to transform raw e-commerce data into clean, analytics-ready datasets.

## Tech Stack
- **Platform:** Databricks
- **Processing:** PySpark, SQL
- **Storage:** Delta Lake
- **Architecture:** Bronze, Silver, Gold
- **Dataset:** Olist Brazilian E-Commerce Dataset
- **Version Control:** Git and GitHub

## Architecture

### Bronze Layer
- Ingest raw source data into Delta tables.
- Preserve source data for downstream processing.
- Support repeatable data ingestion.

### Silver Layer
- Clean and standardize data.
- Handle missing values and duplicates.
- Transform data types and prepare related datasets for analysis.

### Gold Layer
- Build business-ready analytical tables.
- Calculate retail sales and order metrics.
- Enable product category and customer analysis.

## Key Features
- Reusable PySpark ingestion workflows.
- Medallion Architecture implementation.
- Delta Lake table storage.
- SQL-based retail analytics.
- Data quality checks and anomaly detection.

## Dataset
The project uses the Olist Brazilian E-Commerce public dataset. Download the dataset separately and configure the file paths in your Databricks environment.

## Project Status
Project implementation and validation are ongoing. Individual notebooks must be configured and executed in Databricks before the complete pipeline can be considered validated.

## Learning Outcomes
- Data ingestion and ETL pipeline development.
- Distributed data processing with PySpark.
- Data transformation using SQL.
- Lakehouse design with Delta Lake.
- Data quality validation and analytics.

## Author
Kishore Jella
