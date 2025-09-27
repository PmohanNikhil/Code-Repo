from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, when

spark = SparkSession.builder.appName("TaxLookup").getOrCreate()

data1 = [
    (1, "USA", "IND", 'None', 1000.0, "2025-09-04"),
    (2, "GER", "USA", 'None', 500.0, "2025-09-03"),
    (3, "IND", "IND", 'None', 200.0, "2025-09-02"),
    (4, "IND", "CHI", 'None', 200.0, "2025-09-02")
]
columns1 = ["Invoice_ID", "From", "To", "Tax_code", "Amount", "Date"]
df1 = spark.createDataFrame(data1, columns1)

data2 = [
    ("USA", "IND", "T001", 18.0),
    ("GER", "USA", "T002", 15.5),
    ("IND", "IND", "T003", 5.0)
]
columns2 = ["From", "To", "Tax_code", "Tax_percentage"]
df2 = spark.createDataFrame(data2, columns2)

result_df = (
    df1.alias("d1")
    .join(df2.alias("d2"), on=["From", "To"], how="left")
    .select(
        "d1.*",
        col("d2.Tax_code").alias("Lookup_Tax_code"),
        col("d2.Tax_percentage")
    )
)

result_df = result_df.withColumn("Tax_code", col("Lookup_Tax_code")).drop("Lookup_Tax_code")\
                    .withColumn("VAT",when(col("Tax_percentage").isNotNull(), col("Amount") * col("Tax_percentage") / 100).otherwise(0))\
                    .withColumn("Total_Amount", col("Amount") + col("VAT"))

result_df.show(truncate=False)
