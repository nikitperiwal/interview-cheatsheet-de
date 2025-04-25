from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, row_number, desc
from pyspark.sql.window import Window

# --------------------------------------
# SECTION 1: Kafka Streaming Ingestion
# --------------------------------------

# Create Spark Session
spark = SparkSession.builder.appName("kafka_streaming_etl").getOrCreate()

# ------------------------------
# 1.1 JSON Format Ingestion
# ------------------------------

# Stub: Extract schema from JSON record (could be replaced with auto-inferred schema)
def get_json_schema(json_record):
    """
    Infers and returns Spark StructType schema from a JSON record.
    Placeholder function — typically use a sample JSON to infer schema.
    """
    # TODO: Use json.loads() + spark.read.json().schema
    return json_record  # Placeholder return

# Read stream from Kafka topic(s)
stream_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "customer-events")
    .option("startingOffsets", "latest")  # latest or earliest
    .load()
)

# Apply JSON schema to the `value` column (cast from binary to string)
json_schema = get_json_schema(stream_df.select("value").limit(1).collect())
json_parsed_df = stream_df.withColumn("parsed_value", from_json(col("value").cast("string"), json_schema))


# ------------------------------
# 1.2 Avro + Schema Registry Ingestion
# ------------------------------

# Q: How do you ingest Kafka records encoded with Avro and backed by Confluent Schema Registry?

# A: Use Spark-Avro with the Confluent Schema Registry JARs and options.

avro_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "customer-events")
    .option("startingOffsets", "latest")
    .option("kafka.schema.registry.url", "http://localhost:8081")  # Replace with real URL
    .option("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer")
    .option("specific.avro.reader", "true")
    .load()
)

# To deserialize Avro manually (if Spark lacks integration), use fastavro/confluent-kafka-python in a UDF

# Sample UDF usage: decode_avro(value_col) -> parsed StructType

# ------------------------------
# Output Sink and Checkpointing
# ------------------------------

# Write parsed records to Parquet sink with checkpointing
query = (
    json_parsed_df.writeStream
    .option("checkpointLocation", "/checkpoint/path")
    .format("parquet")
    .option("path", "path/to/write")
    .start()
)

# Sample Output Directory Structure:
# /checkpoint/
# /path/to/write/partition_d=2023-xx/

# ------------------------------
# How Offsets Are Managed
# ------------------------------

# Spark uses checkpointing to track the latest Kafka offsets for fault-tolerant recovery.
# Internally, it stores consumer offsets + batch metadata under /checkpoint/

# If you want to use manual consumer-based offset management (advanced), you'd:
# - Use Kafka direct read APIs
# - Store offsets in an external system (e.g., HDFS, Redis, DB)
# - Implement a custom offset commit mechanism (rare in Spark Structured Streaming)

query.awaitTermination()
spark.stop()

# --------------------------------------
# SECTION 2: Downstream Query & Aggregation
# --------------------------------------

# Hive External Table <- Parquet <- Written from Kafka Stream
# Trino <- Hive Table <- Querybook UI

# Sample Query:
# SELECT user_id, SUM(qty) AS total_qty_bought
# FROM customer_data
# GROUP BY user_id;

# --------------------------------------
# SECTION 3: Incremental Merge Logic
# --------------------------------------

# Q: How do you keep adding incremental data to batch data?

# A: Use deduplicated streaming records (latest per user-product combo),
# merge them into Delta tables, and generate new Parquet outputs.
# Older files are marked stale or handled via versioning.

# Deduplicate latest events per key (user_id, product_id)
window_spec = Window.partitionBy("user_id", "product_id").orderBy(desc("timestamp"))
incremental_df = (
    json_parsed_df
    .withColumn("rank", row_number().over(window_spec))
    .filter(col("rank") == 1)
    .select("user_id", "product_id", "qty", "timestamp")
)

# Combine with batch data and write updated partitions
# (Optional) You can mark previous versions as stale using metadata flags

# --------------------------------------
# SECTION 4: How Delta Merge Works Within
# --------------------------------------

# Q: How does Delta merge work internally?

# A: Delta Lake optimizes MERGE INTO via file-level indexing + data skipping.
# Steps:
# 1. Scan Delta table files to find candidate matching keys (via Z-Order or partition)
# 2. For matching keys: compare timestamp to detect newer events
# 3. Perform row-level merge using generated internal transaction logs (_delta_log/)
# 4. Create a new version (snapshot isolation) and update metadata atomically

# Delta Merge Syntax:
# MERGE INTO target_table AS tgt
# USING incremental_df AS src
# ON tgt.user_id = src.user_id AND tgt.product_id = src.product_id
# WHEN MATCHED AND src.timestamp > tgt.timestamp THEN UPDATE SET *
# WHEN NOT MATCHED THEN INSERT *

# Result: Incremental updates are merged into existing dataset without rewriting entire table.

# Delta merge is ACID-compliant and maintains versioning for rollback/audit.

