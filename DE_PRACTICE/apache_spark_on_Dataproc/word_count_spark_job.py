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

# Print word count results
wordCountDF.show()

# Stop the SparkSession
spark.stop()
