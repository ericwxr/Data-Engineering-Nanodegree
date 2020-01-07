import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour,\
     weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')
os.environ['AWS_ACCESS_KEY_ID'] = config.get("AWS", "AWS_ACCESS_KEY_ID")
os.environ['AWS_SECRET_ACCESS_KEY'] = \
    config.get("AWS", "AWS_SECRET_ACCESS_KEY")


def create_spark_session():
    spark = SparkSession.builder\
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0")\
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + "song-data/song_data/*/*/*/*.json"

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    df.createOrReplaceTempView("staging_songs")
    songs_table = spark.sql("""
            SELECT DISTINCT
            song_id   AS song_id,
            title     AS title,
            artist_id AS artist_id,
            year      AS year,
            duration  AS duration
        FROM
            staging_songs
        WHERE song_id IS NOT NULL
    """)

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(
        output_data+"songs-tables/",
        mode="overwrite",
        partitionBy=("year", "artist_id"))

    # extract columns to create artists table
    artists_table = spark.sql("""
        SELECT DISTINCT
            artist_id        AS artist_id,
            artist_name      AS name,
            artist_location  AS location,
            artist_latitude  AS latitude,
            artist_longitude AS longitude
        FROM
            staging_songs
        WHERE artist_id IS NOT NULL
    """)

    # write artists table to parquet files
    artists_table.write.parquet(
        output_data + "artists-tables",
        mode="overwrite")


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + "log-data/*.json"

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.where(df.page == "NextSong")
    df.createOrReplaceTempView("staging_events")

    # extract columns for users table
    artists_table = spark.sql("""
        SELECT DISTINCT
            userId    AS user_id,
            firstName AS first_name,
            lastName  AS last_name,
            gender    AS gender,
            level     AS level
        FROM
            staging_events
        WHERE userId IS NOT NULL
    """)

    # write users table to parquet files
    artists_table.write.parquet(
        output_data + 'users-tables/',
        mode="overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: x/1000.0)
    df = df.withColumn("ts", get_timestamp(df.ts))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x))
    df = df.withColumn("ts", get_datetime(df.ts))

    # extract columns to create time table
    # !!!reload the temp view to incorporate new columns
    df.createOrReplaceTempView("staging_events")
    time_table = spark.sql("""
    SELECT DISTINCT
            ts                      AS start_time,
            hour(ts)                AS hour,
            day(to_date(ts))        AS day ,
            weekofyear(to_date(ts)) AS week,
            month(to_date(ts))      AS month,
            year(to_date(ts))       AS year,
            dayofweek(to_date(ts))  AS weekday
    FROM
        staging_events
    WHERE ts IS NOT NULL
    """)

    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(
        output_data + 'time-tables/',
        mode="overwrite",
        partitionBy=("year", "month"))

    # read in song data to use for songplays table
    song_data = input_data + "song-data/song_data/*/*/*/*.json"
    df = spark.read.json(song_data)
    df.createOrReplaceTempView("staging_songs")

    # extract columns from joined song and log datasets to create
    # songplays table
    songplays_table = spark.sql("""
    SELECT DISTINCT
        se.ts        AS start_time,
        se.userId    AS user_id,
        se.level     AS level,
        ss.song_id   AS song_id,
        ss.artist_id AS artist_id,
        se.sessionId AS session_id,
        se.location  AS location,
        se.userAgent AS user_agent,
        month(to_date(ts)) AS month,
        year(to_date(ts))  AS year
    FROM
        staging_events se
        JOIN
            staging_songs ss
            ON se.song = ss.title
            AND se.artist = ss.artist_name
    """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(
        output_data + 'songplays-tables',
        mode="overwrite",
        partitionBy=("year", "month"))


def main():
    spark = create_spark_session()
    # input_data = "s3a://udacity-dend/"
    input_data = "Data/"
    output_data = "Data/Output/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
