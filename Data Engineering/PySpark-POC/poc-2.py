from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime

spark = SparkSession.builder.appName("poc-2").getOrCreate()

data = [ (1, "Alice", None),
         (2, "Bob", 1), 
        (3, "Charlie", 1),
          (4, "David", 2), 
        (5, "Eva", 2),
          (6, "Frank", 3), 
          (7, "Grace", 3) ] 

columns = ["employee_id", "employee_name", "manager_id"]

input_df = spark.createDataFrame(data, columns)

joined_df = input_df.alias("e1").join(
            input_df.alias("e2"),
            col("e1.manager_id") == col("e2.employee_id"),
            "left"
        ).select(col("e1.employee_id").alias("Employee_id"),
                 col("e1.employee_name").alias("Employee_name"),
                 col("e2.Employee_id").alias("Manager_id"),
                    col("e2.Employee_name").alias("Manager_name"))

joined_df = joined_df.withColumn("date_time", lit(datetime.now()))
joined_df = joined_df.withColumn("date_time", current_timestamp())

# joined_df = joined_df.withColumn("date_time", to_timestamp(col("date_time"),"yyyy-MM-dd HH:mm:ss"))

joined_df.show()

df = joined_df.groupBy(col("Manager_name"))\
              .agg(count(col("Employee_name")).alias("Employee_count"),
                   concat_ws(",", collect_list(col("Employee_name"))).alias("Employee_Names"))\
                .orderBy(col("Manager_name")).filter(col("Manager_name").isNotNull())

print(df.agg(collect_list("Manager_name")).collect())

df.show(truncate=False)


