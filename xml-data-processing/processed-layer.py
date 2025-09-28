import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    filename="app.log",
    filemode="a",
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

try:
    logger.info("Starting the data processing job")
    spark = SparkSession.builder.appName("Raw Data Processing XML").getOrCreate()

    # Input paths
    input_path_complex = r"C:\Users\pmoha\OneDrive\Desktop\GitRepositories\Code-Repo\xml-data-processing\raw_data\raw_data_complex_xml.csv"
    input_path_large = r"C:\Users\pmoha\OneDrive\Desktop\GitRepositories\Code-Repo\xml-data-processing\raw_data\raw_data_large_xml.csv"

    output_folder_complex = r"C:\Users\pmoha\OneDrive\Desktop\GitRepositories\Code-Repo\xml-data-processing\processed_data\complex.csv"
    output_folder_large = r"C:\Users\pmoha\OneDrive\Desktop\GitRepositories\Code-Repo\xml-data-processing\processed_data\large.csv"

    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(output_folder_complex), exist_ok=True)
    os.makedirs(os.path.dirname(output_folder_large), exist_ok=True)

    # ---------- Process Complex XML ----------
    df_complex = spark.read.csv(input_path_complex, header=True)
    df_large = spark.read.csv(input_path_large, header=True)

    sample_complex = df_complex.first()["data"]
    sample_large = df_large.first()["data"]

    print("Sample Complex XML:\n", sample_complex)
    print("\nSample Large XML:\n", sample_large)

    print("Complex CSV first 10 rows:")
    df_complex.show(100, truncate=False)

    df_combined = df_complex.agg(concat_ws("", collect_list("data")).alias("xml_data"))

    xml_string = df_combined.collect()[0]["xml_data"]
    print(xml_string[:500])

    rdd = spark.sparkContext.parallelize([Row(value=xml_string)])

    # Convert RDD to a DataFrame
    df_raw_xml = spark.createDataFrame(rdd)

    df_raw_xml.show(truncate=False)

    df_parsed = spark.read \
                .format("com.databricks.spark.xml") \
                .option("rowTag", "book") \
                .load(xml_string)

    df_parsed.show(truncate=False)
    df_parsed.printSchema()

    # print("\nLarge CSV first 10 rows:")
    # df_large.show(10, truncate=False)

except Exception as e:
    logger.error(f"An error occurred: {e}", exc_info=True)
    raise e