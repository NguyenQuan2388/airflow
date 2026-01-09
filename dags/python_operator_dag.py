"""
Python Operator DAG
This DAG demonstrates the use of PythonOperator with task dependencies
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def print_welcome():
    """Print welcome message"""
    print("Welcome to Airflow!")
    return "Welcome message printed"


def print_date():
    """Print current date and time"""
    print(f"Current date and time: {datetime.now()}")
    return "Date printed"


def print_random_number():
    """Print a random number"""
    import random
    number = random.randint(1, 100)
    print(f"Random number: {number}")
    return number


def print_context(**context):
    """Print the Airflow context"""
    print(f"Execution date: {context['execution_date']}")
    print(f"DAG ID: {context['dag'].dag_id}")
    print(f"Task ID: {context['task'].task_id}")
    return "Context printed"


# Define the DAG
with DAG(
    'python_operator_dag',
    default_args=default_args,
    description='A DAG demonstrating PythonOperator usage',
    schedule_interval='@daily',
    catchup=False,
    tags=['example', 'python'],
) as dag:

    # Create tasks
    welcome_task = PythonOperator(
        task_id='print_welcome',
        python_callable=print_welcome,
    )

    date_task = PythonOperator(
        task_id='print_date',
        python_callable=print_date,
    )

    random_task = PythonOperator(
        task_id='print_random_number',
        python_callable=print_random_number,
    )

    context_task = PythonOperator(
        task_id='print_context',
        python_callable=print_context,
    )

    # Set task dependencies - demonstrating branching and joining
    welcome_task >> [date_task, random_task] >> context_task
