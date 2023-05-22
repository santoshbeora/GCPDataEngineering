from airflow import DAG,Dataset
from airflow.decorators import task
from datetime import datetime

my_file = Dataset(r"D:\DATA ENGINEERING github\DE PRACTICE\udemy - apache airflow\my_file.txt")


with DAG(
    dag_id="consumer",
    schedule=[my_file] , #whe my file will be updated via producer dag this dag will trigger
    start_date=datetime(2023,1,1),
    catchup=False
) as dag:
     
     @task
     def read_dataset():
          with open(my_file.uri,"r") as f:
               print(f.read())

     read_dataset()
