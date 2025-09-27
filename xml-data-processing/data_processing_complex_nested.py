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
    
    
    spark = SparkSession.builder \
        .appName("XML Data Processing") \
        .getOrCreate()

    data_path = r"C:\\Users\\pmoha\\OneDrive\\Desktop\\GitRepositories\\Code-Repo\\xml-data-processing\\complex-nested.xml"

    
    df = spark.read \
        .format("xml") \
        .option("rowTag", "book") \
        .load(data_path)

    df.printSchema()

    df_exploded = df.withColumn("chapter", explode(col("chapters.chapter")))
    df_exploded = df_exploded.withColumn("chapter_paragraph", explode(col("chapter.content.paragraph")))
    df_exploded = df_exploded.withColumn("bio_paragraph", explode(col("author.biography.paragraph")))

    df_flattened = df_exploded.select(
                    col("_category").alias("category"),
                    col("_isbn").alias("isbn"),
                    col("title").alias("book_title"),
                    
                    # Author details
                    col("author.name.first").alias("author_first_name"),
                    col("author.name.last").alias("author_last_name"),
                    col("bio_paragraph").alias("author_biography"),
                    
                    # Publisher details
                    col("publisher.name").alias("publisher_name"),
                    col("publisher.location.city").alias("publisher_city"),
                    col("publisher.location.country").alias("publisher_country"),
                    
                    # Chapter details
                    col("chapter.title").alias("chapter_title"),
                    col("chapter_paragraph").alias("chapter_content")
                )

    output_path = r"C:\\Users\\pmoha\\OneDrive\\Desktop\\GitRepositories\\Code-Repo\\xml-data-processing\\processed_data\\processed_data_complex_xml.csv"
    # df_flattened.write\
    #     .format('parquet') \
    #     .mode("append") \
    #     .save(output_path)

    pdf = df_flattened.toPandas()
    pdf.to_csv(output_path, index=False)
    
    logger.info("Data processing job completed successfully")
except Exception as e:
    logger.error(f"Error occurred: {e}", exc_info=True)
    raise e
