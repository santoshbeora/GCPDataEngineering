from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 6, 10),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Create the DAG object
with DAG(
    dag_id='hello_airflow',
    description='A simple DAG that prints "Hello Airflow!"',
    schedule_interval=timedelta(days=1),
    default_args=default_args
) as dag:
    # Define the tasks
    task_hello = BashOperator(
        task_id='task_hello',
        bash_command='echo "Hello Airflow!"',
        
    )

# Set the task dependencies
task_hello

# You can also define additional tasks and their dependencies here if needed

