from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Spark-Join-Sets") \
    .config("spark.master", "local[*]") \
    .getOrCreate()

# ---------------------------------------------------------------------------------------------------------------------
# JOIN OPERATIONS

# Sample data for DataFrame df1
data1 = [(1, "John"),
         (2, "Jane"),
         (3, "Alice")]

# Sample data for DataFrame df2
data2 = [(1, "Math"),
         (2, "Science"),
         (4, "History")]

# Creating DataFrames
df1 = spark.createDataFrame(data1, ["ID", "Name"])
df2 = spark.createDataFrame(data2, ["ID", "Subject"])

# Show original DataFrames
print("DataFrame 1 Count:", df1.count())
df1.show()

print("DataFrame 2 Count:", df2.count())
df2.show()

# Inner Join - Returns only the rows that have matching values in both
print("Inner Join:")
inner_join = df1.join(df2, on='ID', how='inner')
print("Inner Join Count:", inner_join.count())
inner_join.show()

# Cross Join - Returns the Cartesian product of the two tables (every possible combination of rows from both)
print("Cross Join:")
cross_join = df1.crossJoin(df2)
print("Cross Join Count:", cross_join.count())
cross_join.show()

# Outer Join - Returns all rows from both. If no match, fill with NULLs.
print("Outer Join:")
outer_join = df1.join(df2, on='ID', how='outer')
print("Outer Join Count:", outer_join.count())
outer_join.show()

# Full Outer Join - Similar to an outer join, but it returns all rows regardless of match.
print("Full Outer Join:")
full_outer_join = df1.join(df2, on='ID', how='full_outer')
print("Full Outer Join Count:", full_outer_join.count())
full_outer_join.show()

# Left Join - Returns all rows from the left & matching from right. If no match, fill with NULLs.
print("Left Join:")
left_join = df1.join(df2, on='ID', how='left')
print("Left Join Count:", left_join.count())
left_join.show()

# Right Join - Returns all rows from the right & matching from left. If no match, fill with NULLs.
print("Right Join:")
right_join = df1.join(df2, on='ID', how='right')
print("Right Join Count:", right_join.count())
right_join.show()

# Left Semi Join - Returns all rows from the left where there is a match in the right
print("Left Semi Join:")
left_semi_join = df1.join(df2, on='ID', how='left_semi')
print("Left Semi Join Count:", left_semi_join.count())
left_semi_join.show()

# Left Anti Join - Returns all rows from the left where there is no match in the right
print("Left Anti Join:")
left_anti_join = df1.join(df2, on='ID', how='left_anti')
print("Left Anti Join Count:", left_anti_join.count())
left_anti_join.show()

# Broadcast join
merged_students = df1.join(broadcast(df2), on="ID", how="left")

# ---------------------------------------------------------------------------------------------------------------------
# SET OPERATIONS

# Sample data for DataFrame df1
data1 = [(1, "John"),
         (2, "Jane"),
         (3, "Alice")]

# Sample data for DataFrame df2
data2 = [(3, "Alice"),
         (4, "Bob"),
         (5, "Charlie")]

# Creating DataFrames
df1 = spark.createDataFrame(data1, ["ID", "Name"])
df2 = spark.createDataFrame(data2, ["ID", "Name"])

# Show original DataFrames
print("DataFrame 1:")
df1.show()

print("DataFrame 2:")
df2.show()

# Union
print("Union:")
union_df = df1.union(df2)
union_df.show()

# UnionAll (Same as Union in PySpark, as it includes duplicates)
print("UnionAll:")
union_all_df = df1.unionAll(df2)
union_all_df.show()

# Intersect
print("Intersect:")
intersect_df = df1.intersect(df2)
intersect_df.show()

# Subtract
print("Subtract:")
subtract_df = df1.subtract(df2)
subtract_df.show()

# ---------------------------------------------------------------------------------------------------------------------
