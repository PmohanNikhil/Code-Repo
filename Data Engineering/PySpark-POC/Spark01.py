from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, concat, rand
from pyspark.sql.types import StructField,StructType,StringType


spark = SparkSession.builder.appName("POC_Spark").getOrCreate()

data_ls = [1,2,3,4,5,6,7,8,9,10]
data = [(str(i),) for i in data_ls]

schema = StructType([StructField("id", StringType(), True)])

df = spark.createDataFrame(data, schema)

df = df.withColumn("id_new", concat(col("id").cast("string"),lit("_new_"),(rand() * 1000).cast("int").cast("string")))

df.show()