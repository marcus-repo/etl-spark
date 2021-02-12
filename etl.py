import configparser
import os

from datetime import datetime
from datetime import timedelta

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, asc, desc
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear
from pyspark.sql.functions import row_number, monotonically_increasing_id
from pyspark.sql.window import Window
from pyspark.sql.types import *

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


class job_log(object):
    """ 
        - Class job_log logs process names and startimes of an etl job.
        - Object is initialized with a jobname, a job has a start and end time.
        - Each (process) step is logged with a name and star time.
        - Processing times between steps are calculated and can be printed.
    """
    
    def __init__(self, job_name):
        """ initialize variables"""
        self.job_name = job_name
        self.job_start = None
        self.job_end = None
        self.steps = []
    
    def start(self):
        """ log start time of job. """
        self.job_start = datetime.now()
    
    def end(self):
        """ log end time of job. """
        self.job_end = datetime.now()
    
    def add_step(self, step_name):
        """ add process step to job """
        t = datetime.now()
        print(t, step_name)
        self.steps.append((step_name, t))
        
    def print_summary(self):
        """ calculate processing times and print results"""
        print("")
        print("Job Log Summary")
        print("---------------")
        print("{} \t Start {}".format(self.job_start.strftime("%H:%M:%S"), 
                                      self.job_name))
        for i in range(len(self.steps) - 1):
            print("{} \t {}".format(self.steps[i+1][1] -self.steps[i][1], 
                                    self.steps[i][0] ))
        print("{} \t End {}".format(self.job_end.strftime("%H:%M:%S"), 
                                    self.job_name))
        print("{} \t Total Time".format(self.job_end - self.job_start))  


def create_spark_session():
    """
    Creates and returns spark session.
    """
    spark = SparkSession \
    .builder \
    .config("spark.jars.packages", 
            "org.apache.hadoop:hadoop-aws:2.7.0") \
    .getOrCreate()
    
    spark.conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
    
    return spark
 

def get_songs(spark, input_data):
    """
        Reads song_data from path with given schema.
        Returns spark dataframe.
    """
    
    # path to song files
    songs_data = "song_data/*/*/*/*.json"
    
    # define schema
    songs_schema = StructType([
        StructField("artist_id", StringType(), True),
        StructField("artist_latitude", DoubleType(), True),
        StructField("artist_location", StringType(), True),
        StructField("artist_longitude", DoubleType(), True),
        StructField("artist_name", StringType(), True),
        StructField("duration", DoubleType(), True),
        StructField("num_songs", IntegerType(), True),
        StructField("song_id", StringType(), True),
        StructField("title", StringType(), True),
        StructField("year", IntegerType(), True)
    ])
        
    # read song data files
    songs_df = spark.read.json(input_data + songs_data, schema=songs_schema)
    
    return songs_df


def get_logs(spark, input_data):
    """
        Reads log_data from path with given schema.
        Returns spark dataframe.
    """
    
    # path to log files
    logs_data = "log_data/*/*/*.json"
    #logs_data = "log_data/*.json" # local
    
    # define schema
    logs_schema = StructType([
        StructField("artist", StringType(), True),
        StructField("auth", StringType(), True),
        StructField("firstName", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("itemInSession", IntegerType(), True),
        StructField("lastName", StringType(), True),
        StructField("length", DoubleType(), True),
        StructField("level", StringType(), True),
        StructField("location", StringType(), True),
        StructField("method", StringType(), True),
        StructField("page", StringType(), True),
        StructField("registration", LongType(), True),
        StructField("sessionId", IntegerType(), True),
        StructField("song", StringType(), True),
        StructField("status", IntegerType(), True),
        StructField("ts", LongType(), True),
        StructField("userAgent", StringType(), True),
        StructField("userId", StringType(), True)
    ])
    
    # read logs data files
    logs_df =spark.read.json(input_data + logs_data, schema=logs_schema)
    
    return logs_df


def process_song_data(spark, input_data, output_data, job):
    """
        - Processes songs data
        - Prepares and creates spark dataframes songs_table, artists_table
        - Writes dataframes as parquet files to given output_data location
    """
    
    # read song data file
    job.add_step("Reading song_data")
    songs_df = get_songs(spark, input_data)

    
    # songs_table
    # remove rows without song_id (primary key)
    job.add_step("Preparing songs_table")
    songs_table = songs_df.fillna({"song_id":""}).where(col("song_id") != "")
    
    # remove duplicates in song_id
    songs_window = Window.partitionBy("song_id").orderBy(desc("year"))
    songs_table = songs_table\
        .withColumn("row_num", row_number().over(songs_window))\
        .where(col("row_num") == 1)     
    
    # extract columns to create songs table
    songs_table = songs_table.select(["song_id", "title", "artist_id", 
                                      "year", "duration"])
    
    # write songs table to parquet files partitioned by year and artist
    job.add_step("Writing songs_table")
    songs_table.write.parquet(
        output_data + "songs_table.parquet", 
        mode="overwrite", 
        partitionBy=["year", "artist_id"])    
    
    
    # artists_table
    # remove rows without artitst_id (primary key)
    job.add_step("Preparing artist_table")
    artists_table = songs_df\
        .fillna({"artist_id":""}).where(col("artist_id") != "")
    
    # remove duplicates in artist_id
    artists_window = Window.partitionBy("artist_id").orderBy(desc("year"))
    artists_table = artists_table\
        .withColumn("row_num", row_number().over(artists_window))\
        .where(col("row_num") == 1)
    
    # extract columns to create artists table
    artists_table = artists_table.select(
        "artist_id", 
        col("artist_name").alias("name"), 
        col("artist_location").alias("location"),
        col("artist_latitude").alias("latitude"),
        col("artist_longitude").alias("longitude"))
    
    # write artists table to parquet files
    job.add_step("Writing artists_table")
    artists_table.write.parquet(
        output_data + "artists_table.parquet",
        mode="overwrite")


def process_log_data(spark, input_data, output_data, job):
    """
        - Processes log files and song files
        - Prepares and creates spark dataframes users_table, time_table, 
            songplays_table
        - Joins logs and songs data for songplays_table
        - Writes dataframes as parquet files to given output_data location
    """   
    
    # read log data file
    job.add_step("Reading log_data")
    logs_df = get_logs(spark, input_data)
    
    # filter by actions for song plays
    logs_df = logs_df.where(col("page") == "NextSong")
    #logs_df = logs_df.cache()
    
    
    # users_table
    # remove rows without user_id (primary key)
    job.add_step("Preparing users_table")
    users_table = logs_df.fillna({"userId":""}).where(col("userId") != "")
    
    # remove duplicates in user_id
    users_window = Window.partitionBy("userId").orderBy(desc("ts"))
    users_table = users_table\
        .withColumn("row_num", row_number().over(users_window))\
        .where(col("row_num") == 1)
    
    # extract columns to create users table
    users_table = users_table.select(
        col("userId").alias("user_id"), 
        col("firstName").alias("first_name"), 
        col("lastName").alias("last_name"), 
        "gender", 
        "level")
    
    # write users table to parquet files
    job.add_step("Writing users_table")
    users_table.write.parquet(
        output_data + "users_table.parquet",
        mode="overwrite")
    
    
    # time_table
    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.utcfromtimestamp(x/1000.0), 
                        TimestampType())
    logs_df = logs_df.withColumn("start_time", get_timestamp(col("ts")))
    
    # extract columns to create time table
    job.add_step("Preparing time_table")
    time_table = logs_df.select("start_time").dropDuplicates() \
        .withColumn("year", year("start_time")) \
        .withColumn("month", month("start_time")) \
        .withColumn("day", dayofmonth("start_time")) \
        .withColumn("hour", hour("start_time")) \
        .withColumn("week", weekofyear("start_time"))
   
    # write time table to parquet files partitioned by year and month
    job.add_step("Writing time_table")
    time_table.write.parquet(
        output_data + "time_table.parquet",
        mode="overwrite",
        partitionBy=["year", "month"])
    
   
    # song data
    # read in song data to use for songplays table
    job.add_step("Reading song_data")
    songs_df = get_songs(spark, input_data)
    
    # remove duplicates for join
    job.add_step("Preparing songplays_table")
    songs_df = songs_df.select("title", "artist_id", "song_id", 
                               "artist_name", "duration").dropDuplicates()
    
    
    # songplays_table
    # extract columns from joined song and log data to create songplays table 
    joinExpression = [logs_df.song == songs_df.title, 
                      logs_df.artist == songs_df.artist_name, 
                      logs_df.length == songs_df.duration]
    
    songplays_table = logs_df.join(songs_df, joinExpression)
    songplays_table = songplays_table \
        .withColumn("songplays_id", monotonically_increasing_id()) \
        .withColumn("year", year("start_time")) \
        .withColumn("month", month("start_time")) \
        .select(
            "songplays_id", 
            "start_time", 
            col("userId").alias("user_id"), 
            "level", 
            "song_id", 
            "artist_id", 
            col("sessionId").alias("session_id"), 
            "location", 
            col("userAgent").alias("user_agent"),
            "year",
            "month")
    
    # write songplays table to parquet files partitioned by year and month
    job.add_step("Writing songplays_table")
    songplays_table.write.parquet(
        output_data + "songplays_table.parquet",
        mode="overwrite",
        partitionBy=["year", "month"])
    
    
def main():
    """
        Creates spark session.
        Starts job_log to monitor etl process steps.
        Processes song data.
        Processes long data.
        Ends job log and prints job summary
    """
    
    # create spark session
    spark = create_spark_session()
    
    # path for data in and output
   
    input_data =  "s3a://udacity-dend/"
    output_data = "*"
    
    # process data
    job = job_log("Sparkify")
    job.start()
    
    process_song_data(spark, input_data, output_data, job)    
    process_log_data(spark, input_data, output_data, job)
    
    job.end()
    
    # print job summary
    job.print_summary()


if __name__ == "__main__":
    main()