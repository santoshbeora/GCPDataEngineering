from airflow import DAG,Dataset
from airflow.decorators import task
from datetime import date,datetime
my_file = Dataset(r"D:\DATA ENGINEERING github\DE PRACTICE\udemy - apache airflow\my_file.txt")



with DAG (
    dag_id='producer',
    schedule='@daily',
    start_date=datetime(2023,1,1),
    catchup=False
) as dag:
    
    @task(outlets=[my_file])  # for updating the dataset
    def update_dataset():
        with open (my_file.uri,'a+') as f:
            f.write("Producer Update\n")
    update_dataset()            
