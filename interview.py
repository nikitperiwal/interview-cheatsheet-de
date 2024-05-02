from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Practice-App").config("spark.master", "local[*]").getOrCreate()

# Reading the kafka stream
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.Server", "").option("subscribe", "topic-name").load()

# Transformations
transformed_df = df \
    .withColumn("event_date", to_date("event_ts")) \
    .select("*", "Payload.*").drop("Payload").repartition(1)

(transformed_df.writeStream
 .partitionBy("event_date").format("orc").trigger("5 min").outputMode("append").path("s3://<bucket_nbame>/path/"))

spark.stop()
