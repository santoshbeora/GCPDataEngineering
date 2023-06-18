import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime.now(),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='export_postgres_to_parquet',
    default_args=default_args,
    schedule_interval='@once',
    catchup=True
) as dag:
    

    def export_postgres_to_parquet():
        # Connect to the PostgreSQL database
        pg_hook = PostgresHook(postgres_conn_id="postgre-instance")

        # Query the table and fetch the data
        query = "SELECT * FROM employees"

        # Fetch all rows and column names
        cursor = pg_hook.get_conn().cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]

        if not column_names:
            raise ValueError("No column names found. The query may not have returned any rows.")

        # Convert the data to pandas DataFrame with column names
        df = pd.DataFrame(rows, columns=column_names)

        # Convert DataFrame to Arrow Table
        table = pa.Table.from_pandas(df)

        # Write Arrow Table to Parquet file
        parquet_filename = "gs://personal_projects_by_santosh/exported_data/exported_data.parquet"
        pq.write_table(table, parquet_filename)

    export_task = PythonOperator(
        task_id='export_postgres_to_parquet',
        python_callable=export_postgres_to_parquet
    )

    export_task
