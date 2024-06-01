import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, dayofweek, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType

# Read configuration file
config = configparser.ConfigParser()
config.read("dl.cfg")

os.environ["AWS_ACCESS_KEY_ID"] = config["AWS"]["AWS_ACCESS_KEY_ID"]
os.environ["AWS_SECRET_ACCESS_KEY"] = config["AWS"]["AWS_SECRET_ACCESS_KEY"]
s3_bucket_output = config["AWS"]["S3_BUCKET_OUTPUT"]


def create_spark_session():
    """
        This function creates and returns a SparkSession object with specific configurations.

        The function performs the following steps:
        1. Initializes a SparkSession builder.
        2. Sets the 'spark.jars.packages' configuration to 'org.apache.hadoop:hadoop-aws:2.7.0'.
        3. Sets the 'spark.hadoop.fs.s3a.impl' configuration to 'org.apache.hadoop.fs.s3a.S3AFileSystem'.
        4. Sets the 'spark.hadoop.fs.s3a.awsAccessKeyId' and 'spark.hadoop.fs.s3a.awsSecretAccessKey' configurations using environment variables.
        5. Calls getOrCreate() on the builder to create the SparkSession.

        Returns:
        spark (SparkSession): The created SparkSession object.
    """

    print("create_spark_session")
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.awsAccessKeyId", os.environ['AWS_ACCESS_KEY_ID']) \
        .config("spark.hadoop.fs.s3a.awsSecretAccessKey", os.environ['AWS_SECRET_ACCESS_KEY']) \
        .getOrCreate()
    return spark


def process_song_data(spark,input_data, output_data):
    """
        This function processes song data from a given input source and writes the processed data to a given output source.

        Parameters:
        spark (SparkSession): The SparkSession object to use for data processing.
        input_data (str): The path to the input data source. This should be a directory containing song data in JSON format.
        output_data (str): The path to the output data source. This is where the processed data will be written.

        The function performs the following steps:
        1. Reads song data from the input source.
        2. Extracts relevant columns to create a songs table and an artists table.
        3. Writes the songs table to parquet files, partitioned by year and artist.
        4. Writes the artists table to parquet files.

        The songs table contains the following columns: song_id, title, artist_id, year, duration.
        The artists table contains the following columns: artist_id, artist_name, artist_location, artist_latitude, artist_longitude.
    """

    output_songs = os.path.join(output_data, 'songs/songs_data.parquet')
    output_artists = os.path.join(output_data, 'artists/artists_data.parquet')

    # get filepath to song data file
    print("Load song data")
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')

    # read song data file
    print(f"Read song data file: {song_data}")
    df = spark.read.json(path=song_data)

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration").dropDuplicates(["song_id"])

    # write songs table to parquet files partitioned by year and artist
    print("START write songs table to parquet files")
    songs_table.write.partitionBy('year', 'artist_id').parquet(output_songs, 'overwrite')
    print("END write songs table to parquet files")

    # extract columns to create artists table
    artists_table = df.select("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude").dropDuplicates(["artist_id"])
    
    # write artists table to parquet files
    print("START write artists table to parquet files")
    artists_table.write.parquet(output_artists, 'overwrite')
    print("END write artists table to parquet files")


def process_log_data(spark, input_data, output_data):
    """
        This function processes log data from a given input source and writes the processed data to a given output source.

        Parameters:
        spark (SparkSession): The SparkSession object to use for data processing.
        input_data (str): The path to the input data source. This should be a directory containing log data in JSON format.
        output_data (str): The path to the output data source. This is where the processed data will be written.

        The function performs the following steps:
        1. Reads log data from the input source.
        2. Filters the data for song plays.
        3. Extracts relevant columns to create a users table, a time table, and a songplays table.
        4. Writes the users table, time table, and songplays table to parquet files.

        The users table contains the following columns: userId, firstName, lastName, gender, level.
        The time table contains the following columns: start_time, hour, day, week, month, year, weekday.
        The songplays table contains the following columns: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent, year, month.
    """

    output_users = os.path.join(output_data, 'users/users_data.parquet')
    output_time = os.path.join(output_data, 'time/time_data.parquet')
    output_songplays = os.path.join(output_data, 'songplays/songplays_data.parquet')

    # get filepath to log data file
    print("Load logs data")
    log_data = os.path.join(input_data, 'log_data/*/*/*.json')

    # read log data file
    print(f"Read logs data file: {log_data}")
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table
    artists_table = df.select('userId', 'firstName', 'lastName', 'gender', 'level').dropDuplicates(["userId"])

    # write users table to parquet files
    print("START write users table to parquet files")
    artists_table.write.parquet(output_users, 'overwrite')
    print("END write users table to parquet files")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp((x / 1000)), TimestampType())
    df = df.withColumn("timestamp", get_timestamp(df.ts))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp((x / 1000)), TimestampType())
    df = df.withColumn("datetime", get_datetime(df.ts))

    # extract columns to create time table
    time_table = df.select('datetime') \
        .withColumn('start_time', df.datetime) \
        .withColumn('hour', hour('datetime')) \
        .withColumn('day', dayofmonth('datetime')) \
        .withColumn('week', weekofyear('datetime')) \
        .withColumn('month', month('datetime')) \
        .withColumn('year', year('datetime')) \
        .withColumn('weekday', dayofweek('datetime')) \
        .dropDuplicates(["start_time"])

    # write time table to parquet files partitioned by year and month
    print("START write time table to parquet files")
    time_table.write.parquet(output_time, partitionBy=['year', 'month'], mode='overwrite')
    print("END write time table to parquet files")

    # read in song data to use for songplays table
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')
    song_df = spark.read.json(path=song_data)

    # Create temp view to join 2 data set
    song_df.createOrReplaceTempView("songs")
    df.createOrReplaceTempView("logs")

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = spark.sql("""
                                SELECT monotonically_increasing_id() as songplay_id,
                                       l.timestamp                   as start_time,
                                       l.userId                      as user_id,
                                       l.level                       as level,
                                       s.song_id                     as song_id,
                                       s.artist_id                   as artist_id,
                                       l.sessionId                   as session_id,
                                       l.location                    as location,
                                       l.userAgent                   as user_agent,
                                       year(l.timestamp)             as year,
                                       month(l.timestamp)            as month
                                FROM logs l
                                         JOIN songs s
                                              ON l.song = s.title
                                                  AND l.artist = s.artist_name
                                                  AND l.length = s.duration
                                """)

    # write songplays table to parquet files partitioned by year and month
    print("START write songplays table to parquet files")
    songplays_table.write.partitionBy('year', 'month').parquet(output_songplays, 'overwrite')
    print("END write songplays table to parquet files")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = f"s3a://{s3_bucket_output}/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
