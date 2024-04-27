from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *

# ---------------------------------------------------------------------------------------------------------------------
# INIT + HELPER CODE

# Initialize SparkSession with additional configurations for local run
spark = SparkSession.builder \
    .appName("Spark-Queries-Cheatsheet") \
    .config("master", "local[*]") \
    .getOrCreate()


# Read CSV files into a DataFrame
def read_csv(path):
    return spark.read.csv(path, header=True, inferSchema=True)


# ---------------------------------------------------------------------------------------------------------------------
# READING DATA
students_df = read_csv("../Data/input/csv/students.csv")
program_df = read_csv("../Data/input/csv/program.csv")
scholarship_df = read_csv("../Data/input/csv/scholarship.csv")

students_df.show()
program_df.show()
scholarship_df.show()

# ---------------------------------------------------------------------------------------------------------------------
# QUERIES - 1

# Query to fetch unique values of MAJOR Subjects from Student table.
students_df.select("MAJOR").distinct().show()

# Query to print the first 3 characters of FIRST_NAME from Student table.
students_df.select(substring("FIRST_NAME", 0, 3)).distinct().show()

# Query to find the position of alphabet (‘a’) int the first name column ‘Shivansh’ from Student table.
students_df.filter("FIRST_NAME == 'Shivansh'").select(instr("FIRST_NAME", "a")).show()

# Query that fetches the unique values of MAJOR Subjects from Student table and print its length.
students_df.select("MAJOR", length("MAJOR")).distinct().show()
students_df.groupby("MAJOR").agg({"MAJOR": "len"}).show()

# Query to print FIRST_NAME from the Student table after replacing ‘a’ with ‘A’.
students_df.select(regexp_replace("FIRST_NAME", "a", "A")).show()

# Query to print the FIRST_NAME and LAST_NAME from Student table into single column COMPLETE_NAME.
students_df.select(concat("FIRST_NAME", lit(" "), "LAST_NAME")).show()

# Query to print all Student details order by FIRST_NAME Ascending and MAJOR Subject descending .
students_df.orderBy("MAJOR", ascending=False).orderBy("FIRST_NAME").show()
students_df.orderBy(col("FIRST_NAME").asc(), col("MAJOR").desc()).show()

# Query to print details of the Students excluding FIRST_NAME as ‘Prem’ and ‘Shivansh’.
students_df.filter("FIRST_NAME not in ('Prem', 'Shivansh')").show()

# Query to print details of the Students whose FIRST_NAME ends with ‘a’ and contains five alphabets.
students_df.filter("FIRST_NAME like '%a' and len(FIRST_NAME)=5").show()

# Query to fetch the count of Students having Major Subject ‘Computer Science’.
students_df.filter("MAJOR = 'Computer Science'").groupby("MAJOR").count().show()

# Query to fetch Students full names with GPA >= 8.5 and <= 9.5.
students_df.select(concat("FIRST_NAME", lit(" "), "LAST_NAME"), "GPA").filter("GPA >= 8.5 and GPA<=9.5").show()

# Query to fetch the no. of Students for each MAJOR subject in the descending order.
students_df.groupby("MAJOR").agg(count("*").alias("COUNT")).orderBy("COUNT", ascending=False).show()

# Display the details of students who have received scholarships.
scholarship_df.join(students_df, scholarship_df["STUDENT_REF_ID"] == students_df["STUDENT_ID"], how="left") \
    .select("STUDENT_ID", "FIRST_NAME", "LAST_NAME", "SCHOLARSHIP_AMOUNT", "SCHOLARSHIP_DATE").show()

# Query to show only odd rows from Student table.
students_df.select("*", row_number().over(Window.orderBy("STUDENT_ID")).alias("rn")).filter("rn%2 == 1").show()

# List all students and their scholarship amounts if they have received any. Display NULL if not.
students_df.join(scholarship_df, scholarship_df["STUDENT_REF_ID"] == students_df["STUDENT_ID"], how="left") \
    .select("STUDENT_ID", "FIRST_NAME", "LAST_NAME", "SCHOLARSHIP_AMOUNT", ).show()

# Query to show the top n (say 5) records of Student table order by descending GPA.
students_df.orderBy(desc("GPA")).limit(5).show()

# Query to determine the 5th highest GPA without using LIMIT keyword.
students_df.select("*", row_number().over(Window.orderBy(desc("GPA"))).alias("rn")).filter("rn == 5").show()

# Query to fetch the list of Students with the same GPA.
students_df.alias("a").join(students_df.alias("b"), on="GPA", how="left").filter("a.STUDENT_ID != b.STUDENT_ID").show()

# Query to show one row twice in results from a table.
students_df.union(students_df).show()

# Query to list STUDENT_ID who does not get Scholarship.
students_df.join(scholarship_df, scholarship_df["STUDENT_REF_ID"] == students_df["STUDENT_ID"], how="left") \
    .filter("SCHOLARSHIP_AMOUNT is NULL").select("STUDENT_ID").show()

# Query to fetch the first 50% records from a table.
students_df.select("*", percent_rank().over(Window.orderBy("STUDENT_ID")).alias("rn")).show()

# Query to fetch the MAJOR subject that have less than 4 people in it.
students_df.groupby("MAJOR").agg(count("*").alias("COUNT")).filter("COUNT < 4").show()

# Query to fetch MAJOR subjects along with the max GPA in each of these MAJOR subjects.
students_df.groupby("MAJOR").agg(max("GPA")).show()

# Query to fetch the names of Students who has highest GPA.
max_gpa = students_df.select(max("GPA")).collect()[0][0]
students_df.filter(col("GPA") == max_gpa).select("FIRST_NAME", "LAST_NAME", "GPA").show()

# Query to update the GPA of all the students in ‘Computer Science’ MAJOR subject to 7.5.
students_df.withColumn("GPA", when(col("MAJOR") == "Computer Science", 7.5).otherwise("GPA")).show()

# Query to find the average GPA for each major.
students_df.groupby("MAJOR").agg(avg("GPA")).show()

# Query to find the number of students in each major who have a GPA greater than 7.5.
students_df.filter(col("GPA") >= 7.5).groupby("MAJOR").agg(count("*")).show()

# ---------------------------------------------------------------------------------------------------------------------
