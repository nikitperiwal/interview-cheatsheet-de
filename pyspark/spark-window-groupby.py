from pyspark import StorageLevel
from pyspark.sql.functions import *
from pyspark.sql import SparkSession, Window

# ---------------------------------------------------------------------------------------------------------------------
# INIT CODE

# Initialize SparkSession with additional configurations for local run
spark = SparkSession.builder \
    .appName("Spark-GroupBy-Window-Cheatsheet") \
    .config("spark.master", "local[*]") \
    .getOrCreate()

# Read CSV file into a DataFrame
student_df = spark.read.json("../Data/input/json/base.json")
student_df = student_df.withColumn("Age", lit(2024) - year("DOB"))

# ---------------------------------------------------------------------------------------------------------------------
# GROUP-BY and AGG

# Group by "Grade" and calculate the count of students in each grade
grouped_df = student_df.groupBy("Grade").count()
grouped_df.show()

# Using agg function to calculate multiple aggregates
student_df.groupBy("Grade").agg({
    "Student ID": "max",
    "Age": "avg",
    "Grade": "count"
}).show()

# ---------------------------------------------------------------------------------------------------------------------
# WINDOW FUNCTIONS

# Find the second-oldest person in each grade?
windowSpec = Window.partitionBy("Grade").orderBy(desc("DOB"))
student_df \
    .withColumn("Year_Rank", dense_rank().over(windowSpec)) \
    .filter("Year_Rank == 2") \
    .show()

# Difference of student grade with the max grade
windowSpec = Window.partitionBy("Grade")
student_df \
    .withColumn("Grade_diff", max(col("Student ID")).over(windowSpec) - col("Student ID")) \
    .show()
