
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import requests
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.models import Variable
import ast
import json
from xlsxwriter import Workbook
from dotenv import load_dotenv
import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'forecast_module')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'utils')))
from fetch_data import fetch_data
from process_data import process_data
from insert_data import insert_weather_data
from utils import extract_data_to_xlsx

POSTGRES_CONNECTION_ID = Variable.get("postgres_conn_id")
API_URL = Variable.get('api_url')
api_base_url = Variable.get('forecast_api_base_url')
default_args={
    'owner': 'airflow',
    'depends_on_past': False,
}
api_key = os.getenv('API_KEY')
forecast_data_table_name = Variable.get('forecast_data_table_name')
forecast_export_path = Variable.get('forecast_export_path')
export_path_before_processing = Variable.get('export_path_before_processing')



with DAG('forecast-dag',
         default_args=default_args,
         description='A simple DAG to analyze weather forecast data',
         schedule_interval='@daily',
         start_date=days_ago(1),) as dag:

    fetch_data = PythonOperator(
        task_id='fetch_data',
        python_callable=fetch_data,
        provide_context=True,
        op_kwargs={
            'api_key': api_key,
            'api_base_url': api_base_url
        },
        dag=dag
    )

    process_data_task = PythonOperator(
        task_id='process_data',
        python_callable=process_data,
        provide_context=True,
        op_args={
            'export_path': export_path_before_processing
        },
        dag=dag
    )

    create_table_task = PostgresOperator(  
        task_id='create_table_agg',
        postgres_conn_id=POSTGRES_CONNECTION_ID,
        sql="""
        CREATE TABLE IF NOT EXISTS {{ params.forecast_data_table_name }} (
            dt TIMESTAMP PRIMARY KEY,
            date DATE,
            time TIME,
            min_temperature FLOAT,
            max_temperature FLOAT,
            average_temperature FLOAT, 
            avg_temp_celsius FLOAT,
            temp_change FLOAT,
            humidity INT,
            wind_speed FLOAT,
            wind_category TEXT,
            description TEXT
        );
        """,
    params={
        'forecast_data_table_name': forecast_data_table_name
    }
    )  

    insert_data_task = PythonOperator(
        task_id='insert_weather_data',
        python_callable=insert_weather_data,
        provide_context=True,
        op_kwargs={
            'postgres_conn_id': POSTGRES_CONNECTION_ID
        },
    )

    extract_data_to_xlsx = PythonOperator(
        task_id='extract_data_to_xlsx',
        python_callable=extract_data_to_xlsx,
        provide_context=True,
        op_kwargs={
            'sql_query': "SELECT * FROM {{params.forecast_data_table_name}}",
            'postgres_conn_id': POSTGRES_CONNECTION_ID,
            'export_path': forecast_export_path
        },
        params={
            'forecast_data_table_name': forecast_data_table_name,
        }
    )

    fetch_data >> process_data_task >> create_table_task >> insert_data_task >> extract_data_to_xlsx

