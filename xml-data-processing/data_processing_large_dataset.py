from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging


logging.basicConfig(
    level=logging.INFO,
    filename="app.log",
    filemode="a",
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

try:
    logger.info("Starting the data processing job")
    spark = SparkSession.builder \
                        .appName("Large XML Data Processing")\
                        .getOrCreate()

    input_path = r"C:\\Users\\pmoha\\OneDrive\\Desktop\\GitRepositories\\Code-Repo\\xml-data-processing\\large-dataset.xml"

    input_df = spark.read.format("xml").option("rowTag", "employee").load(input_path)    

    input_df = input_df.withColumn("projects", explode("projects.project"))

    final_df = input_df.select(
        col("id").cast(StringType()),
        col('firstName').cast(StringType()),
        col('lastName').cast(StringType()),
        col('department').cast(StringType()),
        col('hireDate').cast(DateType()),
        col('salary').cast(DoubleType()),
        col('position').cast(StringType()),
        col('projects.*')
    )
    
    pdf = final_df.toPandas()

    output_path = r"C:\\Users\\pmoha\\OneDrive\\Desktop\\GitRepositories\\Code-Repo\\xml-data-processing\\processed_data\\processed_large_dataset.csv"

    pdf.to_csv(output_path, index=False)
    logger.info(f"Data successfully written to {output_path}")
    
except Exception as e:
    logger.error(f"An error occurred: {e}", exc_info=True)
    raise e
