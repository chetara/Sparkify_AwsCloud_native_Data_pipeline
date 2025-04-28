import os
import configparser
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, hour, dayofmonth, weekofyear, month, year, dayofweek, monotonically_increasing_id
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType


def create_spark_session():
    spark = SparkSession.builder \
        .appName("SparkifyETL_Optimized") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.2,com.amazonaws:aws-java-sdk-bundle:1.11.563") \
        .config("spark.sql.shuffle.partitions", "50") \
        .config("spark.default.parallelism", "50") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    hadoop_conf = spark._jsc.hadoopConfiguration()

    # Use environment variables for AWS credentials
    hadoop_conf.set("fs.s3a.access.key", os.getenv('AWS_ACCESS_KEY_ID'))
    hadoop_conf.set("fs.s3a.secret.key", os.getenv('AWS_SECRET_ACCESS_KEY'))
    hadoop_conf.set("fs.s3a.endpoint", "s3.amazonaws.com")

    return spark


    return spark


def process_song_data(spark, input_data, output_data):
    song_data = input_data + "song_data/*/*/*/*.json"

    schema = StructType([
        StructField("artist_id", StringType(), True),
        StructField("artist_latitude", DoubleType(), True),
        StructField("artist_location", StringType(), True),
        StructField("artist_longitude", DoubleType(), True),
        StructField("artist_name", StringType(), True),
        StructField("duration", DoubleType(), True),
        StructField("num_songs", LongType(), True),
        StructField("song_id", StringType(), True),
        StructField("title", StringType(), True),
        StructField("year", LongType(), True)
    ])

    df = spark.read.schema(schema).json(song_data)

    songs_table = df.select("song_id", "title", "artist_id", "year", "duration").dropDuplicates()
    songs_table.coalesce(10).write.partitionBy("year", "artist_id").mode("overwrite").parquet(output_data + "songs/")

    artists_table = df.selectExpr(
        "artist_id",
        "artist_name as name",
        "artist_location as location",
        "artist_latitude as latitude",
        "artist_longitude as longitude"
    ).dropDuplicates()
    artists_table.coalesce(10).write.mode("overwrite").parquet(output_data + "artists/")


def process_log_data(spark, input_data, output_data):
    log_data = input_data + "log_data/*/*/*.json"
    df = spark.read.json(log_data).filter(col("page") == "NextSong")

    users_table = df.selectExpr(
        "userId as user_id",
        "firstName as first_name",
        "lastName as last_name",
        "gender",
        "level"
    ).dropDuplicates()
    users_table.coalesce(1).write.mode("overwrite").parquet(output_data + "users/")

    df = df.withColumn("start_time", from_unixtime(col("ts") / 1000).cast("timestamp"))

    time_table = df.select("start_time") \
        .withColumn("hour", hour("start_time")) \
        .withColumn("day", dayofmonth("start_time")) \
        .withColumn("week", weekofyear("start_time")) \
        .withColumn("month", month("start_time")) \
        .withColumn("year", year("start_time")) \
        .withColumn("weekday", dayofweek("start_time")) \
        .dropDuplicates()
    time_table.coalesce(1).write.partitionBy("year", "month").mode("overwrite").parquet(output_data + "time/")

    song_df = spark.read.parquet(output_data + "songs/").alias("s")
    artist_df = spark.read.parquet(output_data + "artists/").alias("a")
    log_df = df.alias("l")

    songplays_table = log_df \
        .join(song_df, col("l.song") == col("s.title"), "left") \
        .join(artist_df, col("l.artist") == col("a.name"), "left") \
        .selectExpr(
            "l.start_time",
            "l.userId as user_id",
            "l.level",
            "s.song_id as song_id",
            "a.artist_id as artist_id",
            "l.sessionId as session_id",
            "l.location",
            "l.userAgent as user_agent"
        ) \
        .withColumn("songplay_id", monotonically_increasing_id()) \
        .withColumn("year", year("start_time")) \
        .withColumn("month", month("start_time"))

    songplays_table.coalesce(10).write.partitionBy("year", "month").mode("overwrite").parquet(output_data + "songplays/")


def main():
    spark = create_spark_session()

    input_data = "s3a://sparkify-datalake-aws/"
    output_data = "s3a://sparkify-datalake-aws/curated/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)

    print(" ETL pipeline completed — optimized for speed ")


if __name__ == "__main__":
    main()
