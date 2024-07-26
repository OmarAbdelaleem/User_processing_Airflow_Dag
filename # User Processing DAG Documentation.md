# User Processing DAG Documentation
## Overview
This documentation provides a comprehensive guide to the Airflow DAG script designed for processing user data. The script involves tasks such as checking API availability, extracting user data from an API, processing the data, and storing it into a PostgreSQL database.

## Usage:
### To use this DAG:
- Ensure Airflow is installed and configured correctly.
- Install necessary Python packages: `pandas`, `json`, `apache-airflow`, `apache-airflow-providers-postgres`, `apache-airflow-providers-http`.
- Place the DAG script in the dags folder of your Airflow setup.
- Configure the necessary Airflow connections (`postgres` for `PostgreSQL` and `user_api` for the `HTTP API`).
- Trigger the DAG manually or let it run on the defined schedule.

## Import Section
### Overview
This section of the script is dedicated to importing necessary libraries and modules required for the DAG. Each import serves a specific purpose in the script, whether it's for interacting with the database, making HTTP requests, or processing data.

### Details of Imports
`Airflow` Libraries: Used to define and manage DAGs and tasks.


```python
from airflow import DAG
from datetime import datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
```


#### Standard Libraries and Packages: Used for data manipulation and JSON handling.
```python
Copy code
import json
from pandas import json_normalize
```
## Python Functions
`_process_user`:
This function processes the user data extracted from the API.

Parameters:
`ti`: Task instance used to pull data from XCom.

Process:
Fetches user data from the XCom pushed by the extract_user task.
Normalizes and processes the user data.
Saves the processed data to a CSV file.

```python
def _process_user(ti):
    user = ti.xcom_pull(task_ids="extract_user")
    user = user['results'][0]
    processed_user = json_normalize({
        'firstname': user['name']['first'],
        'lastname': user['name']['last'],
        'country': user['location']['country'],
        'username': user['login']['username'],
        'password': user['login']['password'],
        'email': user['email']
    })
    processed_user.to_csv('/tmp/processed_user.csv', index=None, header=False)
```
`_store_user`:
This function stores the processed user data into the PostgreSQL database.

Process:
Establishes a connection to the PostgreSQL database using PostgresHook.
Copies the data from the CSV file into the users table in the database.

```python
def _store_user():
    hook = PostgresHook(postgres_conn_id='postgres')
    hook.copy_expert(
        sql="COPY users FROM stdin WITH DELIMITER as','",
        filename='/tmp/processed_user.csv'
    )
```
## `DAG` Definition
The `DAG` is defined to orchestrate the entire workflow.

```python
with DAG(
    dag_id="user_processing",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:
```
### Tasks
`create_table`: Creates the users table in the PostgreSQL database if it doesn't already exist.

```python
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
```
`is_api_available`: Checks if the user API is available.

```python
is_api_available = HttpSensor(
    task_id='is_api_available',
    http_conn_id='user_api',
    endpoint='api/'
)
```
`extract_user`: Extracts user data from the API.

```python
extract_user = SimpleHttpOperator(
    task_id='extract_user',
    http_conn_id='user_api',
    endpoint='api/',
    method='GET',
    response_filter=lambda response: json.loads(response.text),
    log_response=True
)
```

`process_user`: Processes the extracted user data.

```python

process_user = PythonOperator(
    task_id='process_user',
    python_callable=_process_user
)
```

`store_user`: Stores the processed user data into the PostgreSQL database.

```python
store_user = PythonOperator(
    task_id='store_user',
    python_callable=_store_user
)
```

### Task Dependencies
The task dependencies define the order in which tasks should be executed.

```python
create_table >> is_api_available >> extract_user >> process_user >> store_user
```

This ensures that the table is created first, followed by checking the API availability, extracting user data, processing the data, and finally storing it in the database.