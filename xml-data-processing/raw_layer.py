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
    input_path_complex = r"C:\Users\pmoha\OneDrive\Desktop\GitRepositories\Code-Repo\xml-data-processing\complex-nested.xml"
    input_path_large = r"C:\Users\pmoha\OneDrive\Desktop\GitRepositories\Code-Repo\xml-data-processing\large-dataset.xml"

    # Output folder path
    output_folder_complex = r"C:\Users\pmoha\OneDrive\Desktop\GitRepositories\Code-Repo\xml-data-processing\raw_data\raw_complex"
    output_folder_large = r"C:\Users\pmoha\OneDrive\Desktop\GitRepositories\Code-Repo\xml-data-processing\raw_data\raw_large"

    # Create output directory if it doesn't exist
    os.makedirs(output_folder_complex, exist_ok=True)
    os.makedirs(output_folder_large, exist_ok=True)

    # ---------- Complex XML ----------
    df_raw_complex = spark.read \
                        .format("xml") \
                        .option("rowTag", "book") \
                        .load(input_path_complex)

    df_raw_complex.printSchema()

    output_path_complex = os.path.join(output_folder_complex, "raw_data_complex_xml.json")
    json_rdd = df_raw_complex.toJSON()
    json_list = json_rdd.collect()
    with open(output_path_complex, 'w', encoding='utf-8') as f:
        for json_string in json_list:
            f.write(json_string + '\n')
    logger.info(f"Complex XML raw data saved to {output_path_complex}")

    # ---------- Large XML ----------
    df_raw_large = spark.read.format("xml")\
                    .option("rowTag", "employee")\
                    .load(input_path_large)

    output_path_large = os.path.join(output_folder_large, "raw_data_large_xml.json")
    json_rdd = df_raw_large.toJSON()
    json_list = json_rdd.collect()
    with open(output_path_large, 'w', encoding='utf-8') as f:
        for json_string in json_list:
            f.write(json_string + '\n')
    logger.info(f"Large XML raw data saved to {output_path_large}")

    logger.info("Data processing job completed successfully")

except Exception as e:
    logger.error(f"An error occurred: {e}", exc_info=True)
    raise e
