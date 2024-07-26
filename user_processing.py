from airflow import DAG
from datetime import datetime

# Import necessary operators and hooks from Airflow providers
from airflow.providers.postgres.operators.postgres import PostgresOperator  # type: ignore
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook  # type: ignore

import json
from pandas import json_normalize  # Used for data processing in _process_user

# Python function to process the user data extracted from the API


def _process_user(ti):
    # Fetch data pushed by the previous task extract_user
    user = ti.xcom_pull(task_ids="extract_user")
    user = user['results'][0]

    # Normalize and process the user data
    processed_user = json_normalize({
        'firstname': user['name']['first'],
        'lastname': user['name']['last'],
        'country': user['location']['country'],
        'username': user['login']['username'],
        'password': user['login']['password'],
        'email': user['email']
    })

    # Save the processed user data to a CSV file
    processed_user.to_csv('/tmp/processed_user.csv', index=None, header=False)


# Python function to store the processed user data into the Postgres database
def _store_user():
    # Establish connection to Postgres using PostgresHook
    hook = PostgresHook(postgres_conn_id='postgres')

    # Copy the data from the CSV file to the Postgres table
    hook.copy_expert(
        sql="COPY users FROM stdin WITH DELIMITER as','",
        filename='/tmp/processed_user.csv'
    )


# Define the DAG
with DAG(
    dag_id="user_processing",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    # Task to create the 'users' table in the Postgres database if it doesn't exist
    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres',
        sql='''
            CREATE TABLE IF NOT EXISTS users (
                firstname TEXT NOT NULL,
                lastname TEXT NOT NULL,
                country TEXT NOT NULL,
                username TEXT NOT NULL,
                password TEXT NOT NULL,
                email TEXT NOT NULL
            );
        '''
    )

    # Task to check if the API is available
    is_api_available = HttpSensor(
        task_id='is_api_available',
        http_conn_id='user_api',
        endpoint='api/'
    )

    # Task to extract user data from the API
    extract_user = SimpleHttpOperator(
        task_id='extract_user',
        http_conn_id='user_api',
        endpoint='api/',
        method='GET',
        response_filter=lambda response: json.loads(response.text),
        log_response=True
    )

    # Task to process the extracted user data
    process_user = PythonOperator(
        task_id='process_user',
        python_callable=_process_user
    )

    # Task to store the processed user data into the Postgres database
    store_user = PythonOperator(
        task_id='store_user',
        python_callable=_store_user
    )

    # Define task dependencies
    create_table >> is_api_available >> extract_user >> process_user >> store_user
