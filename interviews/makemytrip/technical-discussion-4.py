# Streaming job
# User context - 2 nodes: session_id, user_id; journey_id
# Page context - 1 value: experiment_id_list, page_name
# Device context - device_id, visitor_id
# Row-based data:
#
# journey_id
# event   - experiment - experiment_list
# event 1 - [1]        - [1]
# event 2 - [2,3]      - [1,2,3]
# event 3 - [4,5]      - [1,2,3,4,5]
#
# Interview question: Modify the streaming job to collect all previous experiments for events
# within the same user_id and journey_id. Ensure that the experiments are accumulated
# incrementally for each event within the same journey_id.

from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import *

# Initialize Spark session
spark = SparkSession.builder.appName("streaming").getOrCreate()

# Read streaming data from Kafka
stream_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "server") \
    .option("subscribe", "list_of_topics").load()


# Function to deserialize Kafka messages (assuming messages are in JSON format)
def deserialize_kafka_message(df):
    # Replace with actual deserialization logic based on your message format
    # For example, if messages are in JSON, you can use from_json function
    return df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")


# Deserialize Kafka messages
deser_df = deserialize_kafka_message(stream_df)

# Sub-question: How can we prevent data skewness since we have a large number of missing journey_id values?
# To prevent data skewness, we can introduce a random salt key for records with missing journey_id values.
# This distributes the records more evenly across partitions.
# Add a salt key for missing journey_id using random value
deser_df = deser_df.withColumn("random_salt", lit(ceil(rand() * 15000))) \
    .withColumn("salt_key", coalesce(col("journey_id"), col("random_salt")))

# Define a window specification for partitioning the data
window_spec = Window.partitionBy("user_id", "journey_id").orderBy("timestamp")

cur_list = set()


# Function to collect unique experiments
def collect_experiments(ex_list):
    global cur_list
    cur_list = set(ex_list).union(cur_list)
    return list(cur_list)


# Register UDF
experiment_udf = udf(collect_experiments)

# Transform the DataFrame to collect experiments within each window
transformed_df = deser_df.withColumn("experiment", experiment_udf("experiment_id_list").over(window_spec))

# Sub-question: Stream-stream join that first attempts to join on user_id and falls back to device_id if user_id is null
# Assuming d1 and d2 are two streaming DataFrames with user_id and device_id columns

# Stream-stream joins typically require defining watermarks to handle late data and avoid infinite state accumulation.
# Watermarks define a threshold for how late data can arrive and still be considered for joining.
# Without watermarks, Spark would have to keep all data in memory indefinitely, which is impractical.

d1 = deser_df.withWatermark("timestamp", "10 minutes")
d2 = deser_df.withWatermark("timestamp", "10 minutes")

# Create a column that prioritizes user_id and uses device_id as a fallback
d1 = d1.withColumn("join_key", when(col("user_id").isNotNull(), col("user_id")).otherwise(col("device_id")))
d2 = d2.withColumn("join_key", when(col("user_id").isNotNull(), col("user_id")).otherwise(col("device_id")))

# Perform stream-stream join with the new join_key
joined_df = d1.join(d2, "join_key", "inner")


# Write the output of the transformed DataFrame to the sink (e.g., console for debugging)
query = transformed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
