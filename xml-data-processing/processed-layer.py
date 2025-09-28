import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
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
    input_path_complex = r"C:\Users\pmoha\OneDrive\Desktop\GitRepositories\Code-Repo\xml-data-processing\raw_data\raw_complex\raw_data_complex_xml.json"
    input_path_large = r"C:\Users\pmoha\OneDrive\Desktop\GitRepositories\Code-Repo\xml-data-processing\raw_data\raw_large\raw_data_large_xml.json"

    output_folder_complex = r"C:\Users\pmoha\OneDrive\Desktop\GitRepositories\Code-Repo\xml-data-processing\processed_data\complex"
    output_folder_large = r"C:\Users\pmoha\OneDrive\Desktop\GitRepositories\Code-Repo\xml-data-processing\processed_data\large"


    # ---------- Process Complex XML ----------
    df_complex = spark.read.json(input_path_complex)

    df_flattened = df_complex.withColumn("chapter", explode_outer("chapters.chapter"))
    df_flattened = df_flattened.withColumn("author_bio_paragraph", 
                                       explode_outer("author.biography.paragraph"))
    
    df_flattened = df_flattened.withColumn("chapter_content_paragraph", 
                                        explode_outer("chapter.content.paragraph"))
    
    df_complex_flattened = df_flattened.select(
        col("_category").alias("category"),
        col("_isbn").alias("isbn"),
        col("author.name.first").alias("author_first_name"),
        col("author.name.last").alias("author_last_name"),
        col("author_bio_paragraph"),
        col("chapter.title").alias("chapter_title"),
        col("chapter_content_paragraph"),
        col("publisher.name").alias("publisher_name"),
        col("publisher.location.city").alias("publisher_city"),
        col("publisher.location.country").alias("publisher_country"),
        col("title").alias("book_title")
    )

    # df_complex_flattened.show(truncate=False)

    # ---------- Process Large XML ----------
    df_large = spark.read.json(input_path_large)

    df_flatten = df_large.withColumn("project", explode_outer("projects.project"))

    df_large_flatten = df_flatten.select(
        col("id").alias("employee_id"),
        col("firstName").alias("First_Name"),
        col("lastName").alias("Last_Name"),
        col("hiredate").alias("date_of_join"),
        col("position").alias("designation"),
        col("department"),
        col("salary"),        
        col("project.name").alias("project_name"),
        col("project.startDate").alias("project_start_date"),
        col("project.endDate").alias("project_end_date")
    )

    windowspec = Window.partitionBy(col("designation"),col("department")).orderBy(col("salary").desc())
    windowspec_cumilative = Window.partitionBy(col("designation"),col("department")).orderBy(col("salary").desc()).rowsBetween(Window.unboundedPreceding, Window.currentRow)

    df_large_flatten = df_large_flatten.withColumn("cumilative_salary", sum("salary").over(windowspec_cumilative)) \
                                        .withColumn("rank", rank().over(windowspec)) \
                                        .withColumn("dense_rank", dense_rank().over(windowspec)) \
                                        .withColumn("row_number", row_number().over(windowspec))

    df_large_flatten.select("designation","department","salary","cumilative_salary","rank","dense_rank","row_number").show(truncate=False)


    # Save processed data
    os.makedirs(output_folder_complex, exist_ok=True)
    output_path_complex = os.path.join(output_folder_complex, "processed_data_complex_xml.csv")
    os.makedirs(output_folder_large, exist_ok=True)
    output_path_large = os.path.join(output_folder_large, "processed_data_large_xml.csv")
    

    df_complex_flattened.toPandas().to_csv(output_path_complex, index=False)
    logger.info(f"Processed complex data saved to {output_path_complex}")

    df_large_flatten.toPandas().to_csv(output_path_large, index=False)
    logger.info(f"Processed large data saved to {output_path_large}")


except Exception as e:
    logger.error(f"An error occurred: {e}", exc_info=True)
    raise e