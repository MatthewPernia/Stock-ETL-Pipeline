from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from run import run_pipeline
import json

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'stock_etl_pipeline',
    default_args=default_args,
    description='Daily ETL pipeline for stock market data',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

def load_config():
    config_path = os.path.join(os.path.dirname(__file__), 'config', 'config.json')
    with open(config_path, 'r') as f:
        return json.load(f)

def etl_task():
    config = load_config()
    run_pipeline(
        symbols=config['symbols'],
        start_date=(datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d'),
        end_date=datetime.now().strftime('%Y-%m-%d'),
        db_config=config
    )

run_etl = PythonOperator(
    task_id='run_etl_pipeline',
    python_callable=etl_task,
    dag=dag,
)

run_etl