from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, split, col, upper

spark = SparkSession.builder.getOrCreate()

data = [("nikhil.pmohan21@ibm.com",),('nikhil.pmohan@tcs.com',)]
df = spark.createDataFrame(data, ["email"])

# Step 1: Remove all digits
df_clean = df.withColumn("email_no_digits", regexp_replace("email", "[0-9]", ""))

# Step 2: Split the email into username and domain
df_split = df_clean.withColumn("parts", split(col("email_no_digits"), "@"))

# Step 3: Further split the username and domain parts
df_final = df_split.select(
    split(col("parts")[0], "\\.")[0].alias("first_name"),
    split(col("parts")[0], "\\.")[1].alias("last_name"),
    upper(split(col("parts")[1], "\\.")[0]).alias("organization")
)

df_final.show()
