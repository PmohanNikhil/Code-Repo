from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *


# Create a SparkSession
spark = SparkSession.builder.appName("Month and country calculation").getOrCreate()

# Create data
data = [
 (1, 'US', 'approved', 1000, '2023-12-18'),
 (2, 'US', 'declined', 2000, '2023-12-19'),
 (3, 'US', 'approved', 2000, '2024-01-01'),
 (4, 'India', 'approved', 2000, '2023-01-07')
]

# Define the schema
schema = StructType([
 StructField("id", IntegerType(), True),
 StructField("country", StringType(), True),
 StructField("status", StringType(), True),
 StructField("amount", IntegerType(), True),
 StructField("date", StringType(), True)
])

[(x,y,z,k,) for x in ls for y in lk for z in lk for k in lk]


df = spark.createDataFrame(data, schema)
df = df.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
df = df.withColumn("Month", date_format(col("date"), "MM"))
df = df.withColumn("Year", date_format(col("date"), "yyyy"))
df.show(truncate=False)

sorted_df = df.groupBy(col("country"),col("Month"),col("Year")).agg(sum(col("amount")).alias("total_amount")\
    ,count(col("status")).alias("total_transaction")\
    ,count(when(col("status") == "approved", 1)).alias("total_approval")\
    ,count(when(col("status") == "declined", 1)).alias("total_decline"))\
    .orderBy(col("country"),col("Month"),col("Year"))
sorted_df.show(truncate=False)

approved_df = df.groupBy(col("country"),col("Month"),col("Year"))\
    .agg(sum(when(col("status") == "approved", col("amount"))).alias("total_approved_amount")\
         ,count(when(col("status") == "approved", 1)).alias("total_approved_transaction"))\
    .orderBy(col("country"),col("Month"),col("Year"))

approved_df.show(truncate=False)

declined_df = df.groupBy(col("country"),col("Month"),col("Year"))\
    .agg(sum(when(col("status") == "declined", col("amount")).otherwise(0)).alias("total_declined_amount")\
         ,count(when(col("status") == "declined", 1)).alias("total_declined_transaction"))\
    .orderBy(col("country"),col("Month"),col("Year"))
declined_df.show(truncate=False)