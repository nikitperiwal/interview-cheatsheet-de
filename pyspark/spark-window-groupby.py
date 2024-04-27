from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *

# ---------------------------------------------------------------------------------------------------------------------
# INIT CODE

# Initialize SparkSession with additional configurations for local run
spark = SparkSession.builder \
    .appName("Spark-GroupBy-Window-Cheatsheet") \
    .config("spark.master", "local[*]") \
    .getOrCreate()

# Read CSV file into a DataFrame
student_df = spark.read.json("../Data/input/json/students.json")
student_df = student_df.withColumn("Age", lit(2024) - year("DOB"))

# ---------------------------------------------------------------------------------------------------------------------
# GROUP-BY and AGG

# Group by "Grade" and calculate the count of students in each grade
grouped_df = student_df.groupBy("Grade").count()
grouped_df.show()

# Using agg function to calculate multiple aggregates
student_df.groupBy("Grade").agg({
    "ID": "max",
    "Age": "avg",
    "Grade": "sum"
}).show()

# Collect list of student names for each grade
list_df = student_df.groupBy("Grade").agg(collect_list("Address.state").alias("Student_Names_List"))
list_df.show(truncate=False)

# Collect set of unique student names for each grade
set_df = student_df.groupBy("Grade").agg(collect_set("Address.state").alias("Student_Names_Set"))
set_df.show(truncate=False)

# ---------------------------------------------------------------------------------------------------------------------
# WINDOW FUNCTIONS

windowSpec = Window.partitionBy("Grade").orderBy(desc("Age"))

# ROW_NUMBER, RANK, DENSE_RANK on Window
student_df \
    .withColumn("Rank", rank().over(windowSpec)) \
    .withColumn("Dense_Rank", dense_rank().over(windowSpec)) \
    .withColumn("Row_Number", row_number().over(windowSpec)) \
    .filter("Grade == 10") \
    .show()

# MAX, MIN, AVG, COUNT on Window
max_df = student_df.withColumn("Grade_diff", max(col("ID")).over(windowSpec) - col("ID"))
max_df.show()

# LAG: Find the previous student in each grade based on age
lag_df = student_df.withColumn("Previous_Student", lag("Name", 1).over(windowSpec))
lag_df.show()

# LEAD: Find the next student in each grade based on age
lead_df = student_df.withColumn("Next_Student", lead("Name", 1).over(windowSpec))
lead_df.show()

# NTILE: Divide students into quartiles based on age within each grade
ntile_df = student_df.withColumn("Age_Quartile", ntile(4).over(windowSpec))
ntile_df.show()

# Find the second-oldest person in each grade?
row_number_df = student_df.withColumn("Year_Rank", row_number().over(windowSpec)).filter("Year_Rank == 2")
row_number_df.show()

# Find the rolling average of age
windowSpec = Window.orderBy("Age").rowsBetween(start=Window.unboundedPreceding, end=0)
student_df.withColumn("Average_Age", avg("Age").over(windowSpec)).show()

# TODO: Add RANGE_BETWEEN

# ---------------------------------------------------------------------------------------------------------------------
