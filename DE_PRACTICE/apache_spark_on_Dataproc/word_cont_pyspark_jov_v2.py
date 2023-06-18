from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split

# Create a SparkSession
spark = SparkSession.builder \
    .appName("WordCount") \
    .getOrCreate()

# Read input text file
inputPath = "gs://dataproc-job-santosh-beora/sample_data/word.text"  # Replace with your input file path
textDF = spark.read.text(inputPath)

# Perform word count
wordCountDF = textDF.select(explode(split(textDF.value, "\\W+")).alias("word")) \
    .groupBy("word") \
    .count()

# Write word count results as a Parquet file to GCS bucket
outputPath = "gs://dataproc-job-santosh-beora/word_count.parquet"  # Replace with your GCS bucket path
wordCountDF.write.parquet(outputPath, mode="overwrite")

# Stop the SparkSession
spark.stop()
