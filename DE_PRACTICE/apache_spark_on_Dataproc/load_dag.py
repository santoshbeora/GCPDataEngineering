from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
from airflow.providers.google.cloud.operators.gcs import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2023, 6, 14),
}

with DAG(
    dag_id='load_parquet_to_bq',
    schedule_interval='@once',
    default_args=default_args,
    catchup=False,
) as dag:
    # Define the GCS and BigQuery configurations
    gcs_bucket = 'personal_projects_by_santosh'
    gcs_object = 'temporary_staging_path/exported_data.parquet'
    bq_dataset = 'p_proj'
    bq_table = 'employees'
    schema_gcs_bucket = 'personal_projects_by_santosh/bq_table_schema'
    schema_gcs_object = 'employees_schema.json'
    project_id = 'project-free-trial-by-santosh'

    # Create the dataset if it doesn't already exist
    create_dataset_task = BigQueryCreateEmptyDatasetOperator(
        task_id='create_dataset',
        dataset_id=bq_dataset,
        project_id=project_id,
        create_disposition='if_not_exists',
    )

    # Create the table if it doesn't already exist
    create_table_task = BigQueryCreateEmptyTableOperator(
        task_id='create_table',
        dataset_id=bq_dataset,
        table_id=bq_table,
        project_id=project_id,
        schema_object=f"gs://{schema_gcs_bucket}/{schema_gcs_object}",
        create_disposition='if_not_exists',
    )

    # Load the Parquet file from GCS to BigQuery
    load_to_bq_task = GCSToBigQueryOperator(
        task_id='load_to_bq',
        bucket=gcs_bucket,
        source_objects=[gcs_object],
        destination_project_dataset_table=f"{project_id}.{bq_dataset}.{bq_table}",
        source_format='parquet',
        write_disposition='WRITE_TRUNCATE',  # Change to 'WRITE_APPEND' if you want to append data
        autodetect=False,
        project_id=project_id,
    )

    # Define the DAG's dependency
    create_dataset_task >> create_table_task >> load_to_bq_task
