from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, dense_rank, sum

# Start a Spark session
spark = SparkSession.builder.appName("WindowFunctionExample").getOrCreate()

# Sample data
data = [
    ("Alice", "HR", 3000),
    ("Bob", "HR", 4000),
    ("Charlie", "IT", 4500),
    ("David", "IT", 5000),
    ("Eve", "HR", 3500),
    ("Frank", "IT", 5500)
]

# Create DataFrame
columns = ["name", "department", "salary"]
df = spark.createDataFrame(data, columns)

# Define a window specification
window_spec = Window.partitionBy("department").orderBy("salary")

# Add window function columns
df_with_window = df \
    .withColumn("row_number", row_number().over(window_spec)) \
    .withColumn("rank", rank().over(window_spec)) \
    .withColumn("dense_rank", dense_rank().over(window_spec)) \
    .withColumn("cumulative_salary", sum("salary").over(window_spec.rowsBetween(Window.unboundedPreceding, Window.currentRow)))

# Show the result
df_with_window.show()
