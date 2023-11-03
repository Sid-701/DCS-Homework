import pandas as pd
import psycopg2
from sqlalchemy import create_engine


def extract_data():
    data = pd.read_json('https://data.austintexas.gov/resource/9t4d-g238.json')
    return data


def transform_data(data):

    # Define the columns for each entity
    animal_columns = ['animal_id', 'name', 'date_of_birth', 'animal_type', 'breed_id']
    outcome_event_columns = ['outcome_event_id', 'datetime', 'animal_id', 'outcome_type']
    fact_table_columns = ['outcome_event_id', 'animal_id', 'breed_id']

    # Your transformation logic here
    data_transformed = data.copy()
    data_transformed.fillna('Not Recorded', inplace=True)
    data_transformed.columns = [col.lower() for col in data_transformed.columns]

    # Create a unique outcome_event_id and breed_id
    data_transformed['outcome_event_id'] = data_transformed.index + 1
    data_transformed['breed_id'] = data_transformed.index + 1

    # Correct duplication for 'animal' and 'outcome_type' tables
    animal_data = data_transformed[animal_columns].drop_duplicates('animal_id', keep='first').reset_index(drop=True)

    unique_breed_type = data_transformed[['breed']].drop_duplicates().reset_index(drop=True)
    unique_breed_type['breed_id'] = unique_breed_type.index + 1
    breed_type = unique_breed_type[['breed_id', 'breed']]

    breed_type_id_map = dict(zip(unique_breed_type['breed'], unique_breed_type['breed_id']))
    data_transformed['breed_id'] = data_transformed['breed'].map(breed_type_id_map)

    # Select only the relevant columns for the outcome_events DataFrame
    outcome_events = data_transformed[outcome_event_columns]

    # Reset the index of the outcome_events DataFrame
    outcome_events.reset_index(drop=True, inplace=True)

    fact_table = data_transformed[fact_table_columns]

    return fact_table, animal_data, outcome_events, breed_type



from sqlalchemy.exc import IntegrityError

def load_data(transformed_data):
    print('Loading data...')
    
    fact_table, animal_data, breed_type, outcome_events = transformed_data

    # Define your DATABASE_URL here

    DATABASE_URL = "postgresql+psycopg2://Sid:123456789$@db:5432/animal_db"

    engine = create_engine(DATABASE_URL)   

    animal_data.to_sql('animal_data', engine, if_exists='append', index=False) 

    breed_type.to_sql('breed_type', engine, if_exists='append', index=False)
 
    outcome_events.to_sql('outcome_events', engine, if_exists='append', index=False)
   
    fact_table.to_sql('fact_table', engine, if_exists='append', index=False)
 
    
    print('Data Loading Completed')



if __name__ == '__main__':
    # Extract data
    extracted_data = extract_data()
    
    # Transform data
    transformed_data = transform_data(extracted_data)
    
    # Load data
    load_data(transformed_data)
