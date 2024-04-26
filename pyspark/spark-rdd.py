from pyspark.sql import SparkSession
from pyspark.sql.types import *

# Initialize SparkSession with additional configurations for local run
spark = SparkSession.builder \
    .appName("StudentCSVAnalysis") \
    .config("spark.master", "local[*]") \
    .config("spark.driver.memory", "1g") \
    .config("spark.executor.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "32") \
    .getOrCreate()

# ---------------------------------------------------------------------------------------------------------------------

# Creating new dataframe
list_df = spark.createDataFrame([1, 2, 3, 4], IntegerType())
list_df.show()

# ---------------------------------------------------------------------------------------------------------------------

# Create Example Data - Departments and Employees

# Create the Employees
Employee = Row("name")  # Define the Row 'Employee' with one column/key
employee1 = Employee('Bob')  # Define against the Row 'Employee'
employee2 = Employee('Sam')  # Define against the Row 'Employee'

# Create the Departments
Department = Row("name", "department")  # Define the Row 'Department' with two columns/keys
department1 = Department('Bob', 'Accounts')  # Define against the Row 'Department'
department2 = Department('Alice', 'Sales')  # Define against the Row 'Department'
department3 = Department('Sam', 'HR')  # Define against the Row 'Department'

# Create DataFrames from rows
employeeDF = spark.createDataFrame([employee1, employee2])
departmentDF = spark.createDataFrame([department1, department2, department3])

# Join employeeDF to departmentDF on "name"
employeeDF.join(departmentDF, "name").show()

# ---------------------------------------------------------------------------------------------------------------------

schema = StructType([
    StructField("letter", StringType(), True),
    StructField("position", IntegerType(), True)])

df = spark.createDataFrame([('A', 0), ('B', 1), ('C', 2)], schema)
df.show()

# ---------------------------------------------------------------------------------------------------------------------

# Stop the SparkSession
spark.stop()

# ---------------------------------------------------------------------------------------------------------------------
