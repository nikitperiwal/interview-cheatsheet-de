# You have immutable data streaming into kafka topic with 10 partitions.
# Now you need to write an application which would read data from the kafka and then after applying some transformations
# You need to store this data into s3/HDFS as a partitioned hive external table.
# This needs to be done in near realtime. E2E lag < 5 min.
# Partition column: Created_date/event_date
#
# Choice of compute engine? Spark-streaming
# Sink Data format - Orc
#
# Transformations:
# Derivation of event_date form the event_ts.
# Payload column is json with 3 keys. Flatten it before storing.

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
