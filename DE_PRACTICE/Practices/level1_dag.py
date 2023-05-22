from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

args = {
    'owner': 'Santosh Beora',
}

with DAG(
    dag_id='hello_world',
    default_args=args,
    schedule_interval='0 0 * * *', #folr running daily at midnight
    start_date=days_ago(1),
) as dag:

    print_hello = BashOperator(
        task_id='print_hello',
        bash_command='echo Hello',
    )

    print_world= BashOperator(
        task_id='print_world',
        bash_command='echo World',
    )

    print_hello >> print_world