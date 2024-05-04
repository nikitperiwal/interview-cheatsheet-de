# We need to write a compaction job which will read small files from Hive external table and then merge them into
# ideal 128MB per file size.
# This needs to be done periodically and the operation must be idempotent.
# 1 record - 100kb / 1 file - 1mb
# (Need to make sure that the data updates are Atomic and don't impact the users)


from pyspark.sql import SparkSession


def create_spark_session():
    return SparkSession.builder \
        .appName("Compaction-job") \
        .config("spark.master", "local[*]") \
        .getOrCreate()


def write(df, path, format, outputmode):
    df.write.format(format).mode(outputmode).save(path)


def get_folder_size(path):
    return 10


def main(db_name, table, start_date, end_date):
    spark = create_spark_session()

    # Reading the data
    df = spark.sql(f"SELECT * FROM {db_name}.{table} WHERE event_dt >= {start_date} and event_dt <= {end_date}")

    repartition_factor = max(1, round(get_folder_size("/path/to/read/") / 128))
    new_df = df.coalesce(repartition_factor)

    # using write we can't directly overwrite the underlying files as spark won't be able to read the other files
    # write(new_df, '/path/to/read/', 'orc', 'overwrite')

    # Using InsertInto
    new_df.write.format("orc").mode("overwrite").partitionBy("event_dt").insertInto("db.table")

    # Using Spark SQL
    new_df.createOrReplaceTempView("new_df")
    spark.sql("INSERT OVERWRITE db.table PARTITION(event_dt) SELECT * FROM new_df")
