from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, expr


data = [
    {"dept_id":101,"e_id":[10101,10102,10103]},
    {"dept_id":102,"e_id":[10201,10202]}
]

spark = SparkSession.builder.appName("jsonpoc_2").getOrCreate()

df = spark.createDataFrame(data)

df = df.withColumn("e_id", explode(col("e_id")))

# df.show()

df = df.withColumn("Expression", expr("concat(dept_id,',',e_id)"))

df.show()