from pyspark.sql import SparkSession

if __name__ == "__main__":
    # Create SparkSession
    spark = SparkSession.builder.appName("SparkSQLJob").getOrCreate()

    # Define the GCS bucket path to store the Parquet file
    gcs_bucket_path = "gs://personal_projects_by_santosh/temporary_staging_path"
    
    #creating dataframe by taking source parquet file
    employees_df=spark.read.parquet("gs://personal_projects_by_santosh/exported_data/exported_data.parquet")

    #converting dataframe to spark table for spark sql work
    employees_df.createOrReplaceTempView("employees")

    # Define the Spark SQL query
    sql_query = """
    SELECT employee_id, first_name, last_name, 
    CONCAT(LOWER(email), '@gmail.com') AS email,
    CONCAT(
        SUBSTRING(phone_number, 1, 3), 
        '-',
        SUBSTRING(phone_number, 5, 3),
        '-',
        SUBSTRING(phone_number, 9, 4)
    ) AS phone_number,
    TO_DATE(hire_date, 'dd-MMM-yy') AS hire_date, job_id, salary, 
    manager_id, department_id
    FROM employees
    """


    # Execute the Spark SQL query
    result_df = spark.sql(sql_query)

    # Write the DataFrame as a Parquet file to the GCS bucket
    #result_df.write.parquet(gcs_bucket_path)
    result_df.write.mode("overwrite").parquet(gcs_bucket_path)

    # Stop the SparkSession
    spark.stop()
