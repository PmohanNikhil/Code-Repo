from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Spark SQL Example").getOrCreate()

transactions_data = [ (101, "T001", 500, "2024-08-10"), (102, "T002", 1200, "2024-08-09"), (103, "T003", 300, "2024-08-08"), (104, "T004", 450, "2024-08-07"), ] 
profile_data = [ (101, "John", 30, "T001"), (102, "Emma", 27, "T005"), (103, "Alex", 35, "T003"), (105, "Sam", 40, "T006"), ]

transactions_df = spark.createDataFrame(transactions_data, ["customer_id", "txn_id", "amount", "txn_date"]) 

profile_df = spark.createDataFrame(profile_data, ["customer_id", "name", "age", "txn_id"])

profile_df = profile_df.withColumnRenamed("txn_id", "last_txn_id")

joined_df = transactions_df.alias("t").join(profile_df.alias("p"), "customer_id", how="full_outer")\
                           .select("t.*","p.name", "p.age", "p.last_txn_id")

joined_df.show(truncate = False)