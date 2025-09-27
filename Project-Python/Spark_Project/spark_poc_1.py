from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Spark POI") \
    .getOrCreate()

print("Spark Session created successfully.")