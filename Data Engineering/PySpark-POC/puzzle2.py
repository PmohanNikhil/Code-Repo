from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("Puzzle1").getOrCreate()

data = [
    ("user1","2019-01-01 00:00:00"),
    ("user1","2019-01-01 09:00:00"),
    ("user1","2019-01-01 10:00:00"),
    ("user2","2019-01-01 00:00:00"),
    ("user2","2019-01-01 12:00:00"),
    ("user2","2019-01-01 13:00:00"),
    ("user3","2019-01-01 00:00:00"),
    ("user3","2019-01-01 12:00:00")
]

df = spark.createDataFrame(data, ["user","current_time"])

window_spec = Window.partitionBy("user").orderBy("current_time")

df = df.withColumn("previous_time", lag("current_time").over(window_spec))

# Calculate the difference in hours between current_time and previous_time

#unix_timestamp is used to convert the timestamp string to seconds since epoch
df = df.withColumn("time_dff_hr", (unix_timestamp("current_time") - unix_timestamp("previous_time"))/3600)

df = df.fillna({"time_dff_hr":0,"previous_time": "N/A"})


df.show(truncate=False)