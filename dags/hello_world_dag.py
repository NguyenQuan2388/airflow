"""
Simple Hello World DAG
This DAG demonstrates basic Airflow functionality with BashOperator
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'hello_world_dag',
    default_args=default_args,
    description='A simple Hello World DAG',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['example', 'hello_world'],
) as dag:

    # Task 1: Print Hello World
    hello_task = BashOperator(
        task_id='say_hello',
        bash_command='echo "Hello World from Airflow!"',
    )

    # Task 2: Print the current date
    date_task = BashOperator(
        task_id='print_date',
        bash_command='date',
    )

    # Task 3: Sleep for a bit
    sleep_task = BashOperator(
        task_id='sleep',
        bash_command='sleep 5',
    )

    # Set task dependencies
    hello_task >> date_task >> sleep_task
