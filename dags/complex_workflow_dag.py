"""
Complex Workflow DAG
This DAG demonstrates a more complex workflow with multiple operators,
parallel execution, and conditional logic
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}


def extract_data():
    """Simulate data extraction"""
    print("Extracting data from source...")
    data = {'records': 100, 'timestamp': datetime.now().isoformat()}
    print(f"Extracted data: {data}")
    return data


def transform_data(**context):
    """Simulate data transformation"""
    print("Transforming data...")
    # In a real scenario, you'd pull data from XCom
    print("Data transformation completed")
    return "transformed_data"


def load_data():
    """Simulate data loading"""
    print("Loading data to destination...")
    print("Data load completed successfully")
    return True


def decide_branch(**context):
    """Decide which branch to take based on some logic"""
    import random
    # Simulate decision logic (in real scenario, this could be based on data quality, size, etc.)
    if random.random() > 0.5:
        return 'process_large_dataset'
    else:
        return 'process_small_dataset'


def cleanup():
    """Cleanup temporary resources"""
    print("Cleaning up temporary files and resources...")
    return "Cleanup completed"


# Define the DAG
with DAG(
    'complex_workflow_dag',
    default_args=default_args,
    description='A complex workflow demonstrating various Airflow features',
    schedule_interval='0 0 * * *',  # Run daily at midnight
    catchup=False,
    tags=['example', 'etl', 'complex'],
) as dag:

    # Start task
    start = EmptyOperator(
        task_id='start',
    )

    # Extract task
    extract = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
    )

    # Data quality check
    data_quality_check = BashOperator(
        task_id='data_quality_check',
        bash_command='echo "Running data quality checks..." && sleep 2',
    )

    # Branch based on data size or quality
    branch = BranchPythonOperator(
        task_id='decide_processing_path',
        python_callable=decide_branch,
        provide_context=True,
    )

    # Path 1: Process large dataset
    process_large = BashOperator(
        task_id='process_large_dataset',
        bash_command='echo "Processing large dataset with distributed computing..." && sleep 3',
    )

    # Path 2: Process small dataset
    process_small = BashOperator(
        task_id='process_small_dataset',
        bash_command='echo "Processing small dataset with single node..." && sleep 2',
    )

    # Transform task
    transform = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        provide_context=True,
        trigger_rule='none_failed_min_one_success',  # Run if at least one parent succeeded
    )

    # Parallel validation tasks
    validate_schema = BashOperator(
        task_id='validate_schema',
        bash_command='echo "Validating data schema..." && sleep 1',
    )

    validate_integrity = BashOperator(
        task_id='validate_integrity',
        bash_command='echo "Validating data integrity..." && sleep 1',
    )

    # Load task
    load = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
    )

    # Cleanup task
    cleanup_task = PythonOperator(
        task_id='cleanup',
        python_callable=cleanup,
    )

    # End task
    end = EmptyOperator(
        task_id='end',
        trigger_rule='none_failed_min_one_success',
    )

    # Define task dependencies
    start >> extract >> data_quality_check >> branch
    branch >> [process_large, process_small] >> transform
    transform >> [validate_schema, validate_integrity] >> load
    load >> cleanup_task >> end
