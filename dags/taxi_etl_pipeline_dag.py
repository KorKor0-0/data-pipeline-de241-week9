from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from ingest_taxi_data import ingest_taxi_data
from clean_taxi_data import clean_taxi_data
from transform_taxi_data import transform_taxi_data
from load_taxi_data import load_taxi_data

# DAG configuration
default_args = {
    "owner": "airflow",
    "retries": 1,
}

with DAG(
    dag_id="nyc_taxi_etl_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["taxi", "etl"],
) as dag:

    # Ingest task
    ingest_task = PythonOperator(
        task_id="ingest_taxi_data",
        python_callable=ingest_taxi_data
    )

    # Clean task
    clean_task = PythonOperator(
        task_id="clean_taxi_data",
        python_callable=clean_taxi_data
    )

    # Transform task
    transform_task = PythonOperator(
        task_id="transform_taxi_data",
        python_callable=transform_taxi_data
    )

    # Load task
    load_task = PythonOperator(
        task_id="load_taxi_data",
        python_callable=load_taxi_data
    )

    # Task dependencies
    ingest_task >> clean_task >> transform_task >> load_task