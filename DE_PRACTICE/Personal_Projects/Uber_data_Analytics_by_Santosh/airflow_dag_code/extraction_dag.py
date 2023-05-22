
from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator


# Set your local file path
local_file_path = r'C:\Users\Santosh\DATA ENGINEERING\Personal Projects\Uber data Analytics - by Santosh/data/uber_data.csv'

# Set your GCS bucket and destination file path
gcs_bucket = "personal_projects_by_santosh"
gcs_destination_path = "data/uber_data.csv"

# Define your DAG
with DAG(
    'upload_local_file_to_gcs',
    description='Upload local file to GCS',
    schedule_interval='@once',
    start_date=datetime(2023, 5, 16),
    catchup=False
)as dag:
    # Define the task
    upload_task = LocalFilesystemToGCSOperator(
        task_id='upload_local_file_to_gcs_task',
        src=local_file_path,
        dst=f"gs://{gcs_bucket}/{gcs_destination_path}",
        bucket=gcs_bucket
    )




