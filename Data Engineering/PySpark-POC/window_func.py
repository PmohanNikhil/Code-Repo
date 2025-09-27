from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, sum, avg, when, count
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("Poc_2").getOrCreate()

data = [(1,"IBM",10000),
        (2,"IBM",20000),
        (3,"IBM",30000),
        (4,"IBM",10000),
        (5,"IBM",50000),
        (6,"IBM",70000),
        (7,"IBM",120000),
        (8,"IBM",120000),
        (9,"IBM",10000),
        (10,"IBM",10000),
        (11,"IBM",8000),
        (12,"IBM",90000)]

schema = StructType([StructField("month", IntegerType(), True),
                     StructField("company", StringType(), True),
                     StructField("revenue", IntegerType(), True)])

df = spark.createDataFrame(data, schema)

window_spec = Window.partitionBy("Company").orderBy(col("month"))

df = df.withColumn("cumilative_sum", sum("revenue").over(window_spec.rowsBetween(Window.unboundedPreceding, Window.currentRow)))

# df = df.withColumn("quater_avg_sum", avg("revenue").over(window_spec.rowsBetween(-2,0)))

# df.show(truncate=False)


df = df.withColumn("Quater_data", when((col("month") >= 1) & (col("month") <=3), "first")\
                   .when((col("month") >= 4) & (col("month") <=6), "second")\
                   .when((col("month") >=7) & (col("month") <=9), "third")\
                   .when((col("month") >=10) & (col("month") <=12), "fourth")\
                   .otherwise("NA"))

df_agg = df.groupBy(col("company"), col("Quater_data")).agg(
    sum(col("revenue")).alias("total_revenue"),
    avg(col("revenue")).alias("avg_revenue")
).orderBy(col("company"), col("total_revenue").desc())

df_agg = df_agg.withColumn("Status", when(col("total_revenue") > col("avg_revenue"), True).otherwise(False))

df_agg = df_agg.withColumn("Status_revenue", when(col("total_revenue") > 150000, True).otherwise(False))


#to show the last 2 records
df_tail = spark.createDataFrame(df_agg.tail(2))
df_tail.show()

# to show the first 2 records
df_head = spark.createDataFrame(df_agg.head(2))

df_head.show()


merge_condtion = " AND ".join(f"target.{col} = source.{col}" for col in keys)

update_clause = ", ".join({col: f"source.{col}" for col in df.coulmns if col != 'created_time'})

insert_clause = ", ".join({col: f"source.{col}" for col in df.columns})

delta_table = DeltaTable.forPath(spark, data_path)

delta_table.alias("target")\
    .merge(input_df.alias("source"), merge_condtion)\
    .whenMatchedupdate(set=update_clause)\
    .whenNotMatchedInsert(values=insert_clause)\
    .execute()


from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("word_count").getOrCreate()

rdd = spark.sparkContext.testFile(path)

word_rdd = rdd.flatMap(lambda line: line.split(""))

word_pair_rdd = word_rdd.map(lambda word: (word,1))

word_count_rdd = word_pair_rdd.reducebyKey(lambda a,b: a+b)

word_count_df = word_count_rdd.toDF(["word","count"])

word_count_df.show()

df.agg(collect_set("revenue")).first()[0]
