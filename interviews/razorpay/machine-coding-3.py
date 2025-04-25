# Suppose you have a stream job where data is coming in periodically (hourly).
# You need to save the data to HDFS/S3. But the data is mutable, so you'll need to update previous data as it comes.
# (We know that streaming jobs don't support updates on HDFS/S3)
# Since the table is continuously being used by analysts, you can have a separate job to do it later.
# We need minimal impact on the users.
#
# Data struct -
# id, data, updated_at, event_date
# payload - updated_at, created_at, event_data
# stream df -> 100 records, static_df -> 10000 records/day
#
# The underlying data is partitioned on event_date, write a CUSTOM logic (can't use delta lake) that will update the
# data for event_date wherever there are updates.

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *


def create_spark_session():
    return SparkSession.builder \
        .appName("Stream-merge-job") \
        .config("spark.master", "local[*]") \
        .getOrCreate()


def read_data(spark, path, format):
    return spark.read.format(format).load(path)


def read_stream(spark, path, format):
    return spark.read.format(format).load(path)


# 1 record - 100kb

def main():
    spark = create_spark_session()

    # Reading the stream_df
    stream_df = read_stream(spark, "path", "orc")

    # Collecting all event_dt
    unique_dt = stream_df.select("event_dt").distinct().collect()

    # Reading static dataframe for the event_dt
    static_df = read_data(spark, "path", "orc") \
        .filter("event_dt in (unique_dt)")

    # Getting latest data
    all_df = stream_df.union(static_df)
    window_spec = Window.partitionBy("id").orderBy("updated_at", ascending=False)
    updated_df = all_df.withColumn("rn", row_number().over(window_spec)).filter("rn==1").drop("rn")

    # Writing data
    updated_df.write.partitionBy("event_dt").mode("overwrite").save("path")
