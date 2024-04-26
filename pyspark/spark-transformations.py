from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# ---------------------------------------------------------------------------------------------------------------------
# INIT CODE

# Initialize SparkSession with additional configurations for local run
spark = SparkSession.builder \
    .appName("Spark-Transformations-Cheatsheet") \
    .config("spark.master", "local[*]") \
    .config("spark.driver.memory", "1g") \
    .config("spark.executor.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "32") \
    .getOrCreate()

# Read CSV file into a DataFrame
student_df = spark.read.json("../Data/input/json/")

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
r1 = student_df.repartition(2, "Student ID")
r2 = student_df.repartition(2)
c1 = student_df.coalesce(2)

# ---------------------------------------------------------------------------------------------------------------------
# TRANSFORMATIONS

# Perform filtering and selection and dropping duplicate
filtered_students = student_df \
    .select("Student ID", "Name", "Address", "Marks", "DOB") \
    .filter("Marks is not NULL") \
    .filter("DOB not like '2014-%'") \
    .filter(student_df["Student ID"] <= 200) \
    .dropDuplicates() \
    .limit(100)

# Create new column "Name" by concatenating first and last names
students_df = filtered_students.withColumn("Random", concat("Name", lit(" "), "Student ID"))

# Derive new age column
students_df = students_df.withColumn("Age", lit(2024) - year("DOB"))

# Rename the "Grade" column to "Student_Grade"
students_df = students_df.withColumnRenamed("Marks", "Student_Grade")

# Replace NULL values in "Student_Grade" with a default value
students_df = students_df.fillna({"Student ID": "100"})

# Convert "DOB" column to date type
students_df = students_df.withColumn("DOB", to_date("DOB", "yyyy-MM-dd"))

# Create new column "Random" based on conditions
students_df = students_df \
    .withColumn("Random", when((trim(col("Name")) == "NULL"), lit(None))
                .otherwise(col("Name").cast("String")))

# Calculate a new column "Is_Adult" based on age
students_df = students_df.withColumn("Is_Adult", when(col("Age") >= 18, "Yes").otherwise("No"))

# Calculate the length of the "Name" column
students_df = students_df.withColumn("Name_Length", length("Name"))

# Convert "Grade" to uppercase
students_df = students_df.withColumn("Name", upper("Name"))

# Add a suffix to the "Name" column
students_df = students_df.withColumn("Suffix_Name", concat(col("Name"), lit("_student")))

# Drop unnecessary columns
students_df = students_df.drop("Random", "Suffix_Name", "Name_Length")

# OrderBy and Show
students_df \
    .orderBy("Age", "Student Id", ascending=False) \
    .show(10)

# ---------------------------------------------------------------------------------------------------------------------
# COMPLEX DATA TRANSFORMATIONS - STRUCTS

# Printing the schema of the df
students_df.printSchema()

# Select individual fields from the "Address" struct column
selected_fields_df = student_df.select("Student ID", "Name", "Address.city", "Address.state", "Address")
selected_fields_df.show()

# Extract all elements from the "Address" struct column
extracted_elements_df = student_df.select("Name", "Address.*")
extracted_elements_df.show()

# Create a struct column combining "city" and "state"
address_struct_df = selected_fields_df.withColumn("Address_New", struct("city", "state"))
address_struct_df.show()

# Add or replace the "zip_code" field in the "Address" struct column
with_zip_code_df = selected_fields_df.withColumn("Address", struct("city", "state", lit("12345").alias("zip_code")))
with_zip_code_df.show(truncate=False)

# Create a nested struct column
nested_struct_df = with_zip_code_df \
    .withColumn("Location",
                struct(struct("city", "state").alias("Address"),
                       "Address.zip_code")
                )
nested_struct_df.show(truncate=False)

# ---------------------------------------------------------------------------------------------------------------------
# COMPLEX DATA TRANSFORMATIONS - ARRAYS

# Explode the "Marks" array column
exploded_df = student_df.select("Student ID", "Name", explode("Marks").alias("Grade"))
exploded_df.show()

# Create a single array from an array of arrays
# flattened_df = student_df.select("Student ID", "Name", flatten("Marks").alias("Grade"))
# flattened_df.show()

# Reduce the "Marks" array column to calculate the sum
reduced_df = exploded_df.groupby("Student ID").sum("Grade")
reduced_df.show()

# Find the minimum grade in the "Marks" array column; Adding sort just because
min_grade = student_df.select("Student ID", "Name", "Marks", array_min(array_sort("Marks", reverse=True)))
min_grade.show()

# Find the maximum grade in the "Marks" array column
max_grade = student_df.select("Student ID", "Name", "Marks", array_max("Marks").alias("Max_Grade"))
max_grade.show()


# ---------------------------------------------------------------------------------------------------------------------
# UDFs (User Defined Functions)

def udf_function(arg):
    # do something
    return arg


udf_func = udf(udf_function, StringType())
udf_result = student_df.withColumn("UDF_COLUMN", udf_func(col("Student ID")))
udf_result.show()

# ---------------------------------------------------------------------------------------------------------------------
