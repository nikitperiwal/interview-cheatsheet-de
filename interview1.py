from pyspark.sql import SparkSession


def create_spark_session():
    return SparkSession.builder \
        .appName("Compaction-job") \
        .config("spark.master", "local[*]") \
        .getOrCreate()


def read_data(spark, path, format):
    return spark.read.format(format).load(path)


def write(df, path, format, outputmode):
    df.write.format(format).mode(outputmode).save(path)


def get_folder_size(path):
    return 10


# 1 record - 100kb

def main(db_name, table, start_date, end_date):
    spark = create_spark_session()

    # Reading the data
    df = spark.sql(f"SELECT * FROM {db_name}.{table} WHERE event_dt >= {start_date} and event_dt <= {end_date}")

    repartition_factor = max(1, round(get_folder_size("/path/to/read/") / 128))
    new_df = df.coalesce(repartition_factor)

    # write(new_df, '/path/to/read/', 'orc', 'overwrite')
    new_df.createOrReplaceTempView("new_df")

    new_df.write.format("orc").mode("overwrite").partitionBy("event_dt").insertInto("db.table")

    spark.sql("INSERT OVERWRITE db.table PARTITION(event_dt) SELECT * FROM new_df")
