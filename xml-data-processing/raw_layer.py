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
    output_folder = r"C:\Users\pmoha\OneDrive\Desktop\GitRepositories\Code-Repo\xml-data-processing\raw_data"

    # Create output directory if it doesn't exist
    os.makedirs(output_folder, exist_ok=True)

    # ---------- Complex XML ----------
    df_raw_complex = spark.read.text(input_path_complex).withColumnRenamed("value", "data")

    output_path_complex = os.path.join(output_folder, "raw_data_complex_xml.csv")
    pdf = df_raw_complex.toPandas()
    pdf.to_csv(output_path_complex, index=False)
    logger.info(f"Complex XML raw data saved to {output_path_complex}")

    # ---------- Large XML ----------
    df_raw_large = spark.read.text(input_path_large).withColumnRenamed("value", "data")

    output_path_large = os.path.join(output_folder, "raw_data_large_xml.csv")
    pdf = df_raw_large.toPandas()
    pdf.to_csv(output_path_large, index=False)
    logger.info(f"Large XML raw data saved to {output_path_large}")

    logger.info("Data processing job completed successfully")

except Exception as e:
    logger.error(f"An error occurred: {e}", exc_info=True)
    raise e
