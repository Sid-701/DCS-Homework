from datetime import datetime, timedelta
import pandas as pd
import requests
import boto3
import requests
import io
import time
import logging
import sys
from dotenv import load_dotenv
import os
import warnings
from sqlalchemy import create_engine
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from Scripts.etl_pipeline import perform_extraction, perform_loading, perform_transformation


warnings.filterwarnings("ignore", category=DeprecationWarning) 

# Define default configuration for the DAG
dag_config = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create DAG instance
data_dag = DAG('austin_shelter_dag',
               default_args=dag_config,
               description='DAGs for ETL Operations',
               schedule_interval=timedelta(days=1))
# Define ETL tasks
extract = PythonOperator(
    task_id='perform_extraction',
    python_callable=extract_data,
    dag=data_dag)

transform = PythonOperator(
    task_id='perform_transformation',
    python_callable=transform_data,
    dag=data_dag)

loading = PythonOperator(
    task_id='perform_loading',
    python_callable=load_data,
    dag=data_dag)

# Set task dependencies
extract >> transform >> loading
