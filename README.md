# Distributed Data Pipeline Using Apache Spark

A Python-based data engineering project that processes sales data using Apache Spark. The pipeline demonstrates data ingestion, transformation, cleaning, and analytics generation.

## Project Overview

This project processes raw sales records, handles data-quality issues, calculates revenue, and generates analytical outputs to support sales reporting.

## Technologies Used

- Python
- Apache Spark (PySpark)
- Pandas
- PyArrow
- Parquet
- Pytest
- Git and GitHub

## Pipeline Workflow

1. **Data Ingestion** — Read sales data from CSV files.
2. **Data Transformation** — Calculate revenue using quantity and unit price.
3. **Data Cleaning** — Handle duplicate records, missing values, invalid quantities and prices, and inconsistent text formatting.
4. **Sales Analytics** — Aggregate revenue by product, category, and city, and generate product-performance results.
5. **Testing** — Validate cleaning logic and analytical results.

## Project Structure

```text
distributed-data-pipeline/
├── data/
│   └── raw/
│       ├── sales.csv
│       └── sales_messy.csv
├── src/
│   ├── ingest.py
│   ├── transform.py
│   ├── clean.py
│   └── analytics.py
├── tests/
│   └── test_pipeline.py
├── requirements.txt
├── run_pipeline.py
├── .gitignore
└── README.md
```

## Setup

Create and activate a Python virtual environment:

```bash
python3 -m venv ddp-venv
source ddp-venv/bin/activate
```

Install dependencies:

```bash
pip install -r requirements.txt
```

## Run the Pipeline

Run the complete workflow:

```bash
python run_pipeline.py
```

Run the tests:

```bash
pytest -v
```

## Data Quality Checks

The cleaning stage demonstrates handling of:

- Duplicate orders
- Missing required fields
- Non-positive quantities and unit prices
- Leading or trailing whitespace in text fields
- Revenue calculation

## Analytics

The analytics stage generates outputs for:

- Revenue by product
- Revenue by category
- Revenue by city
- Product performance

The current implementation runs Spark in local mode. It is a project demonstrating distributed-data-processing concepts; it has not been configured as a multi-node Spark cluster.

## Output

Cleaned data is saved in Parquet format. Analytics outputs are written to the locations configured in `src/analytics.py`.

## Future Improvements

- Add automated data-quality reporting.
- Integrate a persistent data warehouse.
- Schedule pipeline execution.
- Deploy Spark on a multi-node cluster.
- Add visual dashboards for sales analytics.

## Author

Project developed as a hands-on data engineering project using Python and Apache Spark.
