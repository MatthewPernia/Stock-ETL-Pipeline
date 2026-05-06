# Stock ETL Pipeline

A basic ETL pipeline that extracts daily stock market data from Yahoo Finance, transforms it with Pandas and PySpark, and loads it into PostgreSQL. The project also includes an Airflow DAG for daily orchestration and SQL-based trend analysis.

## Features

- Extracts daily stock price data from REST-style API sources via `yfinance`
- Cleans, normalizes, and validates time-series financial data using Pandas
- Supports scalable processing with PySpark for deduplication, joins, and aggregations
- Loads transformed data into PostgreSQL with schema validation and deduplication
- Provides an Airflow DAG for scheduled daily ingestion
- Includes SQL queries for moving averages, daily returns, and data integrity checks
- Configurable to process multiple symbols and daily records per run

## Repository Structure

- `src/`
  - `extract.py` — downloads stock data
  - `transform.py` — cleans and enriches data with Pandas and PySpark
  - `load.py` — inserts data into PostgreSQL and validates schema
  - `run.py` — orchestrates the ETL pipeline
  - `view.py` — runs reporting queries against PostgreSQL
- `config/config.json` — pipeline settings and database credentials
- `data/sql/schema.sql` — PostgreSQL table schema and indexes
- `dags/stock_etl_dag.py` — Airflow DAG definition
- `requirements.txt` — Python dependencies

## Requirements

- Python 3.10+
- PostgreSQL
- `psycopg2-binary`
- `pandas`
- `sqlalchemy`
- `yfinance`
- `pyspark`
- `apache-airflow`

Install dependencies with:

```bash
pip install -r requirements.txt
```

## Configuration

Update `config/config.json` with your PostgreSQL connection details and symbols:

```json
{
  "symbols": ["NVDA", "AAPL", "GOOGL", "MSFT", "TSLA"],
  "start_date": "2024-01-01",
  "end_date": "2025-01-01",
  "db_host": "localhost",
  "db_port": 5432,
  "db_name": "stock_data",
  "db_user": "postgres",
  "db_password": "password",
  "log_path": "pipelines/pipeline.log"
}
```

## Usage

Run the ETL once:

```bash
python src/run.py
```

Run the Airflow DAG by placing `dags/stock_etl_dag.py` in your Airflow DAGs folder and starting Airflow.

## Notes

- Ensure PostgreSQL is running and the target database exists before running the pipeline.
- The pipeline currently uses `yfinance` for extraction and can be extended to other REST APIs.
- The `view.py` script connects to PostgreSQL and prints summary analytics.
