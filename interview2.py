from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *


def create_spark_session():
    return SparkSession.builder \
        .appName("Compaction-job") \
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
