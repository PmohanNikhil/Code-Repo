from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

data = [
    ("2025-01-01","AAPL",150.0),
    ("2025-01-01","GOOGL",2800.0),
    ("2025-01-02","AAPL",152.0),
    ("2025-01-02","GOOGL",2820.0),
    ("2025-01-03","AAPL",155.0),
    ("2025-01-03","MSF",2850.0),
    ("2025-01-04","AAPL",158.0),
    ("2025-01-04","MSF",2900.0),
    ("2025-01-05","AAPL",160.0),
    ("2025-01-05","MSF",2950.0)
]

spark = SparkSession.builder.appName("poc").getOrCreate()

df = spark.createDataFrame(data,['date','product', 'price'])

window_spec = Window.partitionBy("product").orderBy("date")

df = df.withColumn("avg_daily_price", avg("price").over(window_spec))

df_agg = df.groupBy("product").agg(max("avg_daily_price").alias("max_avg_daily_price"))

df.show(truncate=False)
df_agg.show(truncate=False)