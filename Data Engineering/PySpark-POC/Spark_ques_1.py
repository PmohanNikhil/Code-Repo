from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("Name").getOrCreate()

sales_data = [
 ("Telangana", "Hyderabad", "Anil", 120000, "Jan"),
 ("Telangana", "Hyderabad", "Thiviksha", 125000, "Jan"),
 ("Telangana", "Hyderabad", "Raj", 125000, "Jan"),
 ("Telangana", "Hyderabad", "Priya", 110000, "Jan"),
 ("Maharashtra", "Pune", "Amit", 95000, "Jan"),
 ("Maharashtra", "Pune", "Sneha", 98000, "Jan"),
 ("Maharashtra", "Pune", "Vikas", 98000, "Jan"),
 ("Karnataka", "Bengaluru", "Kiran", 130000, "Jan"),
 ("Karnataka", "Bengaluru", "Ravi", 128000, "Jan"),
 ("Karnataka", "Bengaluru", "Theekshana", 128000, "Jan"),
 ("Karnataka", "Bengaluru", "Divya", 127000, "Jan"),
 ("Tamil Nadu", "Chennai", "Hari", 90000, "Jan"),
 ("Tamil Nadu", "Chennai", "Mani", 92000, "Jan"),
 ("Tamil Nadu", "Chennai", "Radha", 89000, "Jan"),
]

df = spark.createDataFrame(sales_data, ["State", "City", "Name", "Sales", "Month"])

window_spec = Window.partitionBy("State", "City").orderBy(col("Sales").desc())

df = df.withColumn("Dense_Rank", dense_rank().over(window_spec))

top_empl_df = df.filter(col("Dense_Rank") <=2).orderBy(col("State"), col("City"), col("Dense_Rank"))

top_empl_df.show()