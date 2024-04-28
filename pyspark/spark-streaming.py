from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Spark-Streaming") \
    .getOrCreate()

# Define the schema for the streaming data
schema = "value STRING"

# Read streaming data from Kafka topic
streaming_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "your_topic_name") \
    .load() \
    .selectExpr("CAST(value AS STRING)")  # Assuming the value is a string

# Perform processing on the streaming data (e.g., splitting words)
words_df = streaming_df.select(
    explode(split(streaming_df.value, " ")).alias("word")
)

# Define the query to write the processed data to console
query = words_df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Start the query execution
query.awaitTermination()
