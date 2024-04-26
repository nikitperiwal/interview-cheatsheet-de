from pyspark import StorageLevel
from pyspark.sql.types import StringType
from pyspark.sql.functions import *
from pyspark.sql import SparkSession

# ---------------------------------------------------------------------------------------------------------------------
# INIT CODE

# Initialize SparkSession with additional configurations for local run
spark = SparkSession.builder \
    .appName("StudentCSVAnalysis") \
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

# Perform filtering and selection
filtered_students = student_df \
    .select("Student ID", "Name", "Address", "Grades", "DOB") \
    .filter("Grades is not NULL") \
    .filter("DOB not like '2014-%'") \
    .filter(student_df["Student ID"] <= 200) \
    .limit(100)

# Create new column "Name" by concatenating first and last names
students_df = filtered_students.withColumn("Random", concat("Name", lit(" "), "Student ID"))

# Derive new age column
students_df = students_df.withColumn("Age", lit(2024) - year("DOB"))

# Rename the "Grade" column to "Student_Grade"
students_df = students_df.withColumnRenamed("Grades", "Student_Grade")

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

students_df = students_df.orderBy("Student Id", ascending=False)

# Show the Dataframe
students_df.show(10, truncate=False)

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

# Explode the "Grades" array column
exploded_df = student_df.select("Student ID", "Name", explode("Grades").alias("Grade"))
exploded_df.show()

# Create a single array from an array of arrays
# flattened_df = student_df.select("Student ID", "Name", flatten("Grades").alias("Grade"))
# flattened_df.show()

# Reduce the "Grades" array column to calculate the sum
reduced_df = exploded_df.groupby("Student ID").sum("Grade")
reduced_df.show()

# Find the minimum grade in the "Grades" array column
min_grade = student_df.select("Student ID", "Name", "Grades", array_min("Grades").alias("Min_Grade"))
min_grade.show()

# Find the maximum grade in the "Grades" array column
max_grade = student_df.select("Student ID", "Name", "Grades", array_max("Grades").alias("Max_Grade"))
max_grade.show()

# ---------------------------------------------------------------------------------------------------------------------
# UDFs (User Defined Functions)

def udf_function(arg):
    # do something
    return arg


udf_func = udf(udf_function, StringType())
udf_result = student_df.withColumn("New_Column", udf_func(col("Student ID")))
udf_result.show()

# ---------------------------------------------------------------------------------------------------------------------
