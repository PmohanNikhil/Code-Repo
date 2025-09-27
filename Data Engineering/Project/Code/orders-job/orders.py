import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, count, sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql.window import Window

spark = SparkSession.builder\
            .appName("orders_job")\
            .getOrCreate()

type_mapping = {
    "string": StringType(),
    "integer": IntegerType(),
    "float": FloatType()    
}

config_path = "C:\\Users\\nikhi\\OneDrive\\Desktop\\Project\\Code\\Configuration\\config.json"
with open(config_path, 'r') as config_file:
    config = json.load(config_file)
    
input_path = config.get("input_path")
output_path = config.get("output_path")

print(f"Processing the data on environment: {config.get('env')}")

order_schema_json = config.get("orders")
order_items_schema_json = config.get("order_items")

order_schema = StructType([
    StructField(field["name"], type_mapping[field["type"]], field["nullable"])
    for field in order_schema_json
])

order_items_schema = StructType([
    StructField(field["name"], type_mapping[field["type"]], field["nullable"])
    for field in order_items_schema_json
])


order_items_df = spark.read\
                    .option("header","false")\
                    .option("delimiter",",")\
                    .csv("C:\\Users\\nikhi\\OneDrive\\Desktop\\Project\\retail_db-master\\retail_db-master\\order_items\\part-00000.csv", schema = order_items_schema)

orders_df = spark.read\
                    .option("header","false")\
                    .option("delimiter",",")\
                    .csv("C:\\Users\\nikhi\\OneDrive\\Desktop\\Project\\retail_db-master\\retail_db-master\\orders\\part-00000.csv", schema=order_schema)

grouped_order_items = order_items_df.groupBy(col("order_item_id"))\
                                    .agg(sum(col("amount")).alias("total_amount"),
                                         count(col("order_id")).alias("total_items"))\
                                    .orderBy(col("order_item_id"))

grouped_order_items = grouped_order_items.withColumn("cost_per_item", when(col("total_items") > 0, col("total_amount") / col("total_items")).otherwise(lit(0)))
grouped_order_items.show(truncate=False)