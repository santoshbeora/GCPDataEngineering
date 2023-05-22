import os
import glob
from airflow import DAG
from google.cloud import storage
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'Santosh Beora',
    'start_date': datetime(2023, 5, 15),
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='load_dag',
    default_args=default_args,
    schedule_interval='@once'
) as dag:
    
    gcs_bucket = 'personal_projects_by_santosh'
    gcs_prefix = 'temporary_staging_path/'
    folder_path = f'gs://{gcs_bucket}/{gcs_prefix}'

    # Instantiate a GCS client
    client = storage.Client()
    # Get the GCS bucket
    bucket = client.get_bucket(gcs_bucket)
    blobs = bucket.list_blobs(prefix=gcs_prefix)
    csv_files = [blob.name for blob in blobs if blob.name.endswith('.csv')]


    project_id = 'project-free-trial-by-santosh'
    dataset_id = 'project_uber_analytics'
    
   
    
    if len(csv_files) == 0:
        raise ValueError("No CSV files found in the specified folder path.")
    
    for file in csv_files:
        filename = os.path.basename(file)
        table_id = os.path.splitext(filename)[0]
        gcs_object = f"{gcs_prefix}{filename}"
        task_id = f'load_csv_{table_id}_to_bq'
        destination_table = f"{dataset_id}.{table_id}"
        
        load_csv_task = GCSToBigQueryOperator(
            task_id=task_id,
            bucket=gcs_bucket,
            source_objects=[gcs_object],
            destination_project_dataset_table=f'{project_id}.{dataset_id}.{table_id}',
            autodetect=True,
            create_disposition='CREATE_IF_NEEDED',
            write_disposition='WRITE_TRUNCATE'
        )
        
        
    
