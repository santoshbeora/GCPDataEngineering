from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime

with DAG('parallel_dag', start_date=datetime(2022, 1, 1), schedule_interval='@daily', catchup=False) as dag:    
    with TaskGroup('airflow-worker-69685f7547-pvwwm_Tasks', prefix_group_id=False) as worker1_tasks:
        extract_a = BashOperator(
            task_id='extract_a',
            bash_command='sleep 10',
            node='airflow-worker-69685f7547-pvwwm'
        )
 
        load_a = BashOperator(
            task_id='load_a',
            bash_command='sleep 10',
            node='airflow-worker-69685f7547-pvwwm'
        )
 
   
        extract_b = BashOperator(
            task_id='extract_b',
            bash_command='sleep 10',
            node='airflow-worker-69685f7547-pvwwm'
        )
 
        load_b = BashOperator(
            task_id='load_b',
            bash_command='sleep 10',
            node='airflow-worker-69685f7547-pvwwm'
        )
    with TaskGroup('Worker2_Tasks', prefix_group_id=False) as worker2_tasks:
        transform = BashOperator(
           task_id='transform',
           bash_command='sleep 30',
           node='airflow-worker-69685f7547-m45h7'
        )

    
    extract_a >> load_a
    extract_b >> load_b
    [load_a, load_b] >> transform
