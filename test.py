from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.appName("app").getOrCreate()


schema = StructType([
    StructField("ID", IntegerType(), False),
    StructField("Name", StringType(), False),
    StructField("Address", StringType(), True),
])
data = [
    (1, "Aman", "India"),
    (2, "Brian", "US"),
    (3, "Chloe", None),
]

df = spark.createDataFrame(data, schema)
df.show()

spark.stop()