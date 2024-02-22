'''
=============================================
Milestone 3

Name    : Mohammad Dandy Goesti 
Batch   : BSD-003

This program was created to automate the transformation and load of data from PostgreSQL to ElasticSearch. 
The dataset used is a dataset regarding car accident in Kensington and Chelsea (January 2021).

=============================================
'''
# Library used for the program
import datetime as dt
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import pandas as pd
import psycopg2 as db
from elasticsearch import Elasticsearch


# 1. FETCH DATA FROM POSTGRESQL
    # define Fetch_from_Postgresql() as function to pull data from postgresql
def Fetch_from_Postgresql():
    db_name = 'airflow'
    db_user = 'airflow'
    db_password = 'airflow'
    db_host = 'postgres'
    db_port = '5432'
    # using psycopg2 as db to connect to postgresql
    connection = db.connect(
        database = db_name,
        user = db_user,
        password = db_password,
        host = db_host,
        port = db_port,
    )

    # Get data from table_m3 with limit 30.000
    select_query = 'SELECT * FROM table_m3 LIMIT 30000;'
    df = pd.read_sql(select_query, connection)
    # CLOSE CONNECTION
    connection.close()
    # Saving data to csv as data_raw
    df.to_csv('/opt/airflow/dags/P2M3_dandy_data_raw.csv')
    

# 2. DATA CLEANING
    # define function to clean raw_data
def Data_Cleaning():
    # Open the previous saved data
    df = pd.read_csv('/opt/airflow/dags/P2M3_dandy_data_raw.csv')
    # Drop duplicate data
    df.drop_duplicates(inplace=True)
           
    # Relabeling the data in columns
    accident_mapping = {
        'Fatal':'Fatal',
        'Fetal':'Fatal',
        'Slight':'Slight',
        'Serious':'Serious'
    }
    df['Accident_Severity'] = df['Accident_Severity'].map(accident_mapping)
        
    #  Relabel the values and fill missing values in 'Weather_Conditions' 
    conditions_mapping = {
        'Dry': 'Fine no high winds',
        'Wet or damp': 'Raining no high winds',
        'Snow': 'Snowing no high winds',
        'Frost or ice': 'Snowing + high winds'
    }
    df['Weather_Conditions'] = df['Weather_Conditions'].map(conditions_mapping)

    # Fill remaining missing values in 'Weather_Conditions' with 'Other'
    df['Weather_Conditions'].fillna('Other', inplace=True)

    # Relabel values in 'Vehicle_Type' column to specified categories
    category_mapping = {
        'Car': 'Car',
        'Taxi/Private hire car': 'Taxi',
        'Motorcycle over 500cc': 'Motorcycle',
        'Van / Goods 3.5 tonnes mgw or under': 'Van',
        'Goods over 3.5t. and under 7.5t': 'Other',
        'Motorcycle 125cc and under': 'Motorcycle',
        'Motorcycle 50cc and under': 'Motorcycle',
        'Bus or coach (17 or more pass seats)': 'Bus',
        'Goods 7.5 tonnes mgw and over': 'Other',
        'Other vehicle': 'Other',
        'Motorcycle over 125cc and up to 500cc': 'Motorcycle',
        'Agricultural vehicle': 'Other',
        'Minibus (8 - 16 passenger seats)': 'Other',
        'Pedal cycle': 'Pedal cycle',
        'Ridden horse': 'Ridden horse'
    }
    df['Vehicle_Type'] = df['Vehicle_Type'].map(category_mapping)

    # Drop unbalanced columns since too many missing values and this column is not really usefull
    df.drop(columns='Carriageway_Hazards', inplace=True)

    # Drop unwanted columns
    unwanted_columns = ['Latitude', 'Longitude', 'Junction_Control', 'Local_Authority_(District)', 'Police_Force']
    df.drop(columns=unwanted_columns, inplace=True)
    
    # Clean column name by lowercasing and replacing white space between column name          
    df.columns = df.columns.str.lower().str.replace(' ', '_')         
    # Drop Missing Values
    df.dropna(inplace=True)

    # SAVE NEW CSV as data_cleaned
    df.to_csv('/opt/airflow/dags/P2M3_dandy_data_cleaned.csv')


# 3. INSERT INTO ELASTIC SEARCH
    # define function to insert data into elastic
def Post_to_Elasticsearch():
    # Open the previous saved data_cleaned
    df=pd.read_csv('/opt/airflow/dags/P2M3_dandy_data_cleaned.csv')
    
    # Check connection
    es = Elasticsearch("http://elasticsearch:9200") 
    print('Connection status : ', es.ping())

    # insert csv file to elastic search
    for i,r in df.iterrows():
        # chage doc to json format
        doc=r.to_json()
        # Specifies the Elasticsearch index where the document should be indexed.
        res=es.index(index="milestone_3_sql",doc_type="doc",body=doc)
        print(res)

# SET DATA PIPELINE
default_args = {
    'owner': 'dandy',
    'start_date': dt.datetime(2024, 1, 24), # set start date of fetching data
    'retries': 1, # set retries to 1
    'retry_delay': dt.timedelta(minutes=1), # if failed will retry in 1 minutes
}

# SET DAG criteria
with DAG('P2M3_dandy_DAG', # using this DAG
         default_args=default_args,
         schedule_interval='30 6 * * *',  # This means run at 6:30 AM every day
         ) as dag:

# These tasks below define workflow where data is fetched from PostgreSQL (Fetch_from_Postgresql), 
# cleaned (Data_Cleaning), and then posted to Elasticsearch (Post_to_Elasticsearch).
# Each of these tasks is a node in the directed acyclic graph (DAG) that represents the workflow. 
    getData = PythonOperator(task_id='Fetch_from_Postgresql',
                                 python_callable=Fetch_from_Postgresql)
    
    cleanData = PythonOperator(task_id='Data_Cleaning',
                               python_callable=Data_Cleaning)
    
    insertData = PythonOperator(task_id='Post_to_Elasticsearch',
                                 python_callable=Post_to_Elasticsearch)


# Define the execution order
getData >> cleanData >> insertData
