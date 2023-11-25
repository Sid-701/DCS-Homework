import pandas as pd
import requests
import boto3
import requests
import io
import time
import logging
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta


load_dotenv("./dags/secrets.env")

AWS_URL = os.getenv('AWS_URL')
ACCESS_KEY = os.getenv('ACCESS_KEY')
SECRET_KEY = os.getenv('SECRET_KEY')
BUCKET_NAME = os.getenv('BUCKET_NAME')
DB_PORT = os.getenv('DB_PORT')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')
DB_HOST = os.getenv('DB_HOST')
DB_URL = os.getenv('DB_URL')

DB_URL = DB_URL.format(DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME)

def load_data():
    # Initialize S3 client
    s3_client = boto3.client(
        's3',
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY
    )

    # Download data from S3 and load into DataFrames
    files_to_load = ['cleaned/animal.csv', 'cleaned/outcomes.csv', 'cleaned/outcomes_events.csv', 'cleaned/data.csv']
    data_frames = {}
    for file_to_load in files_to_load:
        obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=file_to_load)
        data_frames[file_to_load] = pd.read_csv(io.BytesIO(obj['Body'].read()))

    # Create an engine instance
    database_engine = create_engine(DB_URL)

    # Upload DataFrames to PostgreSQL
    data_frames['cleaned/animal.csv'].to_sql('animal', database_engine, if_exists='append', index=False)
    data_frames['cleaned/outcomes.csv'].to_sql('outcome_type', database_engine, if_exists='append', index=False)
    data_frames['cleaned/outcomes_events.csv'].to_sql('outcome_event', database_engine, if_exists='append', index=False)
    data_frames['cleaned/data.csv'].to_sql('fact_table', database_engine, if_exists='append', index=False)

def transform_data():
    s3_client = boto3.client(
        's3',
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY
    )
    bucket = BUCKET_NAME
    s3_object = 'original_data/austin_animal_outcomes.csv'

    # Download the file from S3
    s3_object_data = s3_client.get_object(Bucket=bucket, Key=s3_object)
    data_frame = pd.read_csv(io.BytesIO(s3_object_data['Body'].read()))

    # Perform transformations
    transformed_data = data_frame.copy()  # Example transformation

    # Transformations
    transformed_data.fillna('Not Recorded', inplace=True)
    transformed_data['outcome_type_id'] = transformed_data.index + 1
    transformed_data['outcome_event_id'] = transformed_data.index + 1

    # Dividing into entities
    animal_table_cols = ['animal_id', 'breed', 'color', 'name', 'date_of_birth', 'animal_type']
    outcome_table_cols = ['outcome_type_id', 'outcome_type']
    outcome_event_cols = ['outcome_event_id', 'datetime', 'sex_upon_outcome', 'outcome_subtype', 'animal_id',
                          'outcome_type']
    data_columns_order = ['animal_id', 'outcome_type_id', 'outcome_event_id']

    # Re-ordering
    animal_data = transformed_data[animal_table_cols]
    outcomes_data = transformed_data[outcome_table_cols]
    outcome_events_data = transformed_data[outcome_event_cols]
    transformed_data = transformed_data[data_columns_order]

    # Correcting Duplication
    animal_data.drop_duplicates(inplace=True)
    outcomes_data = pd.DataFrame(pd.Series(outcomes_data['outcome_type'].unique(), name='outcome_type'))
    outcomes_data['outcome_type_id'] = outcomes_data.index + 1
    outcomes_data = outcomes_data[['outcome_type_id', 'outcome_type']]
    outcomes_data_2 = outcomes_data[['outcome_type', 'outcome_type_id']]

    dictionary_of_outcomes = dict(zip(outcomes_data_2['outcome_type'], outcomes_data_2['outcome_type_id']))
    outcome_events_data['outcome_type_id'] = outcome_events_data['outcome_type'].map(dictionary_of_outcomes)

    outcome_events_data = outcome_events_data.drop('outcome_type', axis=1)

    transformed_data["outcome_type_id"] = transformed_data['outcome_type'].map(dictionary_of_outcomes)
    transformed_data = transformed_data.drop('outcome_type', axis=1)

    # Uploading to the cloud
    # Upload transformed data back to S3
    output_buffer = io.StringIO()
    transformed_data.drop_duplicates(inplace=True)
    transformed_data.to_csv(output_buffer, index=False)
    s3_client.put_object(Bucket=bucket, Key='cleaned/data.csv', Body=output_buffer.getvalue())

    output_buffer = io.StringIO()
    animal_data.drop_duplicates(inplace=True)
    animal_data.to_csv(output_buffer, index=False)
    s3_client.put_object(Bucket=bucket, Key='cleaned/animal.csv', Body=output_buffer.getvalue())

    output_buffer = io.StringIO()
    outcomes_data.drop_duplicates(inplace=True)
    outcomes_data.to_csv(output_buffer, index=False)
    s3_client.put_object(Bucket=bucket, Key='cleaned/outcomes.csv', Body=output_buffer.getvalue())

    output_buffer = io.StringIO()
    outcome_events_data.drop_duplicates(inplace=True)
    outcome_events_data.to_csv(output_buffer, index=False)
    s3_client.put_object(Bucket=bucket, Key='cleaned/outcomes_events.csv', Body=output_buffer.getvalue())

def extract_data():
    # AWS S3 client initialization
    s3_client = boto3.client(
        's3',
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY
    )

    # Delete existing data in 'original_data' folder
    objects_to_delete = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix='original_data/')
    if 'Contents' in objects_to_delete:
        delete_keys = {'Objects': [{'Key': obj['Key']} for obj in objects_to_delete['Contents']]}
        s3_client.delete_objects(Bucket=BUCKET_NAME, Delete=delete_keys)

    # Retrieve new data
    new_data_frame = pd.read_json(AWS_URL)
    new_data_frame = new_data_frame[['animal_id', 'name', 'datetime', 'monthyear', 'date_of_birth',
                                     'outcome_type', 'animal_type', 'sex_upon_outcome', 'age_upon_outcome',
                                     'breed', 'color', 'outcome_subtype']]

    # Check for existing data in 'outdated' folder
    outdated_key = 'outdated/austin_animal_outcomes.csv'
    try:
        old_object = s3_client.get_object(Bucket=BUCKET_NAME, Key=outdated_key)
        old_data_frame = pd.read_csv(io.BytesIO(old_object['Body'].read()))
    except s3_client.exceptions.NoSuchKey:
        old_data_frame = pd.DataFrame(
            columns=['animal_id', 'name', 'datetime', 'monthyear', 'date_of_birth',
                     'outcome_type', 'animal_type', 'sex_upon_outcome', 'age_upon_outcome',
                     'breed', 'color', 'outcome_subtype'])  # If no old data, create an empty DataFrame

    # Compare and get only new or updated records
    combined_data_frame = pd.concat([new_data_frame, old_data_frame])
    combined_data_frame.drop_duplicates(keep=False, inplace=True)
    updated_data_frame = combined_data_frame[~combined_data_frame.index.isin(old_data_frame.index)]

    if updated_data_frame.empty:
        logging.info("No new or updated data to upload. Creating an empty CSV with headers.")
        updated_data_frame = pd.DataFrame(columns=new_data_frame.columns)

    # Upload data to 'original_data'
    logging.info("Uploading data to 'original_data'...")
    csv_buffer = io.StringIO()
    updated_data_frame.to_csv(csv_buffer, index=False)
    file_content = csv_buffer.getvalue().encode()
    s3_client.put_object(Bucket=BUCKET_NAME, Key='original_data/austin_animal_outcomes.csv', Body=file_content)

    # Update 'outdated' with current data
    logging.info("Updating 'outdated' with current data...")
    csv_buffer_old = io.StringIO()
    new_data_frame.to_csv(csv_buffer_old, index=False)
    old_file_content = csv_buffer_old.getvalue().encode()
    s3_client.put_object(Bucket=BUCKET_NAME, Key=outdated_key, Body=old_file_content)
