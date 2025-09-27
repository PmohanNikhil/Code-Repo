from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, from_json
from pyspark.sql.types import StructType, StructField, StringType

# Step 1: Initialize SparkSession
spark = SparkSession.builder.appName("FlattenJSON").getOrCreate()
# schema = StructType(
#     [
#         StructField("coffee",StringType(),True),
#         StructField("brewing", StringType(), True)
#     ]
# )

# df = df.withColumn("data", from_json(col("value"), schema))

# Step 2: Sample JSON as a Python dictionary (for this example)
data = [{
    "coffee": {
        "region": [
            {"id": 1, "name": "John Doe"},
            {"id": 2, "name": "Don Joeh"}
        ],
        "country": {"id": 2, "company": "ACME"}
    },
    "brewing": {
        "region": [
            {"id": 1, "name": "John Doe"},
            {"id": 2, "name": "Don Joeh"}
        ],
        "country": {"id": 2, "company": "ACME"}
    }
}]


df = spark.read.json(spark.sparkContext.parallelize(data))

brewing_df = df.select("brewing.*")
brewing_df = brewing_df.withColumn("region", explode(col("region")))

brewing_df = brewing_df.select(
    col("region.id").alias("region_id"),
    col("region.name").alias("region_name"),
    col("country.id").alias("country_id"),
    col("country.company").alias("country_company")
)

brewing_df.show(truncate=False)


coffe_df = df.select("coffee.*")

coffe_df = coffe_df.withColumn("region", explode(col("region")))

coffe_df = coffe_df.select(col("region.*"), col("country.*"))

coffe_df.show(truncate=False)