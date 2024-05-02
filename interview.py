from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Practice-App").config("spark.master", "local[*]").getOrCreate()

spark.stop()
