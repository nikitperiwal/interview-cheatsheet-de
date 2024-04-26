from pyspark.sql import SparkSession

# Initialize SparkSession with additional configurations for local run
spark = SparkSession.builder \
    .appName("Spark-Read-Write-Cheatsheet") \
    .config("spark.master", "local[*]") \
    .getOrCreate()

# ---------------------------------------------------------------------------------------------------------------------
# READING DATA

# Read CSV file into a DataFrame
csv_df = spark.read \
    .option("delimiter", ",") \
    .csv("../Data/input/csv/", header=True, inferSchema=True)

# Additional ways to read data from various formats:

# Read JSON files
json_df = spark.read.json("../Data/input/json")

# Read ORC files
orc_df = spark.read.orc("../Data/input/orc")

# Read from SQL query
# sql_df = spark.sql("""SELECT * FROM databricks.citydata""")

# Read from a specific format
format_df = spark.read \
    .option("delimiter", ",") \
    .format("csv").load("../Data/input/csv")

# ---------------------------------------------------------------------------------------------------------------------
# WRITING DATA

# Writing data to CSV format
csv_df \
    .write \
    .format("csv") \
    .option("header", "true") \
    .mode("overwrite") \
    .save("../Data/output/csv")

# Writing data to ORC format
csv_df \
    .write \
    .format("orc") \
    .partitionBy("Grade") \
    .mode("overwrite") \
    .save("../Data/output/orc")
