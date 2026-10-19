# Distributed Data Pipeline

## Project Overview

A data engineering project that uses Apache Spark to ingest, clean, transform, and analyze sales data. The pipeline generates analytics datasets in Parquet format.

## Technologies Used

- Python 3.12
- Apache PySpark 4.2.0
- PyArrow
- pytest
- Parquet

## Project Structure

```text
distributed-data-pipeline/
├── data/
│   ├── raw/
│   │   ├── sales.csv
│   │   └── sales_messy.csv
│   └── processed/
│       └── clean_sales.parquet
├── src/
│   ├── ingest.py
│   ├── clean.py
│   ├── transform.py
│   └── analytics.py
├── tests/
│   └── test_pipeline.py
├── requirements.txt
└── run_pipeline.py
```

## Pipeline Stages

1. **Ingestion:** Read raw sales data from CSV files.
2. **Cleaning:** Remove duplicate orders, missing values, and invalid quantities or prices.
3. **Transformation:** Calculate revenue and prepare the data for analysis.
4. **Analytics:** Generate product, category, and city revenue summaries.
5. **Storage:** Save processed data and analytics results in Parquet format.
6. **Testing:** Validate cleaning logic, output datasets, and revenue calculations.

## How to Run

Activate the virtual environment:

```bash
source ~/ddp-venv/bin/activate
```

Run the pipeline:

```bash
python run_pipeline.py
```

Run the tests:

```bash
python -m pytest -v
```

## Output Datasets

- `revenue_by_product`
- `revenue_by_category`
- `revenue_by_city`
- `product_performance`

Analytics outputs are saved under `~/ddp-output/`.

## Current Status

- Data ingestion implemented.
- Data cleaning implemented.
- Sales analytics implemented.
- Parquet outputs generated.
- Three automated tests passing.
