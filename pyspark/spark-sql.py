from pyspark import StorageLevel
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession, Window

# Initialize SparkSession with additional configurations for local run
spark = SparkSession.builder \
    .appName("StudentCSVAnalysis") \
    .config("spark.master", "local[*]") \
    .config("spark.driver.memory", "1g") \
    .config("spark.executor.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "32") \
    .getOrCreate()

# ---------------------------------------------------------------------------------------------------------------------
# READING

# Read files from other formats
# json_df = spark.read.json("/path/to/folder/")
# orc_df = spark.read.orc("/path/to/folder/")
# sql_df = spark.sql("""SELECT * FROM databricks.citydata""")
# format_df =spark.read.format("csv").load("path/to/data/")

# Read the CSV file into a DataFrame
student_df = spark.read \
    .option("delimiter", ",") \
    .csv("Data/input/", header=True, inferSchema=True)

# ---------------------------------------------------------------------------------------------------------------------
# COMMON FUNCTIONS

# Show the original DataFrame
student_df.show()

# Show the schema
student_df.printSchema()

# Persist the dataframe
student_df.persist(StorageLevel.MEMORY_AND_DISK)

# Un-persist the dataframe
student_df.unpersist()

# Create a temp view
student_df.createOrReplaceTempView("temp_view")
student_df.createOrReplaceGlobalTempView("global_view")

# Repartition & Coalesce the data
r1 = student_df.repartition(2, "Grade")
r2 = student_df.repartition(2)
c1 = student_df.coalesce(2)

# ---------------------------------------------------------------------------------------------------------------------
# TRANSFORMATIONS

## EXPODE ARRAY & STRUCT;
# explode json , array ,struct

# Perform some transformations
grade_students = student_df \
    .select("Student ID", "First Name", "Last Name", "Grade", "DOB") \
    .filter("Grade is not NULL") \
    .filter("DOB not like '2014-%'") \
    .filter(student_df["Student ID"] <= 200) \
    .limit(100)

# Creating new columns and using if statements
grade_students = grade_students \
    .withColumn("Name", concat("First Name", lit(" "), "Last Name")) \
    .withColumn("Random", when((trim(col("First Name")) == "NULL"), lit(None))
                .otherwise(col("First Name").cast("String"))) \
    .drop("First Name", "Last Name", "Random")

# GroupBy and count
grade_students_count = grade_students \
    .groupby("Grade") \
    .count()

grade_students_count.show()

# ---------------------------------------------------------------------------------------------------------------------
# WINDOW FUNCTIONS

# Find the second-oldest person in each grade?
windowSpec = Window.partitionBy("Grade").orderBy(desc("DOB"))
grade_students \
    .withColumn("Year_Rank", dense_rank().over(windowSpec)) \
    .filter("Year_Rank == 2") \
    .show()

# Difference of student grade with the max grade
windowSpec = Window.partitionBy("Grade")
grade_students \
    .withColumn("Grade_diff", max(col("Student ID")).over(windowSpec) - col("Student ID")) \
    .show()

# ---------------------------------------------------------------------------------------------------------------------
# JOINS, MERGE, UNIONS

# Show the joined DataFrame
merged_students = grade_students.join(broadcast(grade_students_count), on="Grade", how="left")

merged_students.sort("Grade", "Student ID", ascending=False).show()
merged_students.orderBy("Grade", "Student ID", ascending=False).show()

# Union ALL - All rows
merged_students.unionAll(merged_students).show()

# Union - Unique rows
merged_students.union(merged_students).show()

# Get aggregate table
agg_students = merged_students \
    .groupby("Grade") \
    .agg({"Grade": "count"}) \
    .orderBy("Grade", ascending=False)

agg_students.show()

# ---------------------------------------------------------------------------------------------------------------------
# WRITING DATA

# Write data to csv
grade_students \
    .write \
    .format("csv") \
    .option("header", "true") \
    .mode("overwrite") \
    .save("Data/output/csv")

# Write data to orc
grade_students \
    .write \
    .format("orc") \
    .partitionBy("Grade") \
    .mode("overwrite") \
    .save("Data/output/orc")

# ---------------------------------------------------------------------------------------------------------------------

# Stop the SparkSession
spark.stop()
