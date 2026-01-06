from airflow import DAG
from datetime import datetime, timedelta
# Fixed: added 's' to operators
from airflow.operators.python import PythonOperator
import sys
import os

# Adds the Cloud Composer DAGs folder to the path so it can find your cleaning script
sys.path.append(os.environ.get('DAGS_FOLDER', '/home/airflow/gcs/dags'))

# Import the logic from your other file
from Cleaned_data_pandas_numpy import run_cleaning

default_args = {
    'owner' : 'airflow',
    'depends_on_past' : False,
    # Fixed: changed timedelta to datetime
    'start_date' : datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay' : timedelta(minutes=5),
}

with DAG('coffee_data_cleaning_v1',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    clean_task = PythonOperator(
        task_id = 'clean_coffee_sales_data',
        python_callable=run_cleaning,
        op_kwargs={
            'input_path': 'gs://my-coffee-data-bucket/raw/Coffe_sales.xlsx',
            'output_path': 'gs://my-coffee-data-bucket/processed/Coffee_sales_clean.csv'
        }
    )