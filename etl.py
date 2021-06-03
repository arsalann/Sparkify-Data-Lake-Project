import configparser
from datetime import datetime, date
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek, monotonically_increasing_id
from pyspark.sql.types import TimestampType, DateType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']= config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']= config['AWS']['AWS_SECRET_ACCESS_KEY']



# Starts a new Spark Session
def create_spark_session():
    print('\n\Spark Session Starting...\n\n')
    '''
    Function: 
        1) initialize spark session builder
        2) configure session parameters
        3) create or get session if exists

    Parameters:
        None
    
    Returns:
        spark session curser
    '''
    spark = SparkSession \
        .builder \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.0') \
        .getOrCreate()
    return spark


# Process song data files
def process_song_data(spark, input_data, output_data):
    print('\n\nProcessing Song Data...\n\n')
    '''
    Function: 
        1) loads input JSON files from S3: song_data 
        2) extracts data columns
        3) write songs and artists parquet files to S3

    Parameters:
        spark: the cursor
        input_path: path to the S3 bucket with song data
        output_path: path to S3 bucket where parquet files are stored
    
    Returns:
        None
    '''

    

    ## STEP 1 - LOAD INPUT DATA

    # get filepath to song data file
    # song_data = input_data+'/song_data/*/*/*/*.json'
    song_data = input_data+'/song_data/A/A/A/*.json'

    # read song data file
    df = spark.read.json(song_data)
    print('Completed: input file song_data loaded from S3')



    ## STEP 2 - EXTRACT DIMENSIONAL COLUMNS

    # songs - songs in music database
        # song_id, title, artist_id, year, duration
    
    # extract columns to create songs table
    songs_table = df.select(
        'song_id', 'title', 'artist_id', 'year', 'duration'
        ).dropDuplicates()
    print('Completed: songs_table columns extracted')

    # artists - artists in music database
        # artist_id, name, location, lattitude, longitude
    
    # extract columns to create artists table
    artists_table = df.select(
        'artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude'
        ).dropDuplicates()
    print('Completed: artists_table columns extracted')



    ## STEP 3 - WRITE PARQUET FILES

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data+'/songs_table',
                              mode='overwrite',
                              partitionBy=['year', 'artist_id'])
    print('Completed: songs_table written to S3')

    # write artists table to parquet files
    artists_table.write.parquet(output_data+'/artists_table',
                              mode='overwrite')
    print('Completed: artists_table written to S3')


    print('\n\nCompleted: song data processed!\n\n')



# Process log data files
def process_log_data(spark, input_data, output_data):
    print('\n\nProcessing Log Data...\n\n')
    '''
    Function: 
        1) loads input JSON files from S3: log_data 
        2) extracts data columns - dimension tables
        3) extracts data columns - fact table
        4) write user and time parquet files to S3
    Parameters:
        spark: the cursor
        input_path: path to the S3 bucket with song data
        output_path: path to S3 bucket where parquet files are stored
    Returns:
        None
    '''

    ## STEP 1 - LOAD INPUT DATA

    # get filepath to song data file
    # log_data = input_data+'/log_data/*/*/*/*.json'
    log_data = input_data+'/log_data/2018/11/*.json'

    # read in song data to use for songplays table
    # song_data = input_data+'/song_data/*/*/*/*.json'
    song_data = input_data+'/song_data/A/A/A/*.json'
    dfSong = spark.read.json(song_data)
    
    # read song data file
    df = spark.read.json(log_data)
    # filter by actions for song plays
    df = df.filter(col('page') == 'NextSong')
    print('Completed: input files log_data and song data loaded from S3')
    


    ## STEP 3 - EXTRACT DIMENSIONAL COLUMNS

    # users - users in the app
        # userId, first_name, last_name, gender, level
    users_table = df.select(
        'ts', 'userId', 'firstName', 'lastName', 'gender', 'level'
    ).dropDuplicates()
    print('Completed: users_table columns extracted')
    
    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda epoch: datetime.fromtimestamp(epoch / 1000.0), TimestampType())
    df = df.withColumn('start_time', get_timestamp('ts'))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda epoch: date.fromtimestamp(epoch / 1000.0), DateType())
    df = df.withColumn('date', get_datetime('ts'))

    # extract columns to create time table
    df = df.withColumn('date_year', year('date'))
    df = df.withColumn('date_month', month('date'))
    
    time_table = df.select('start_time', 
                           hour('date').alias('date_hour'), 
                           dayofmonth('date').alias('date_day'), 
                           weekofyear('date').alias('date_week'), 
                           month('date').alias('date_month'),
                           year('date').alias('date_year'),
                           dayofweek('date').alias('date_weekday')
                        ).distinct()
    
    

    ## STEP 2 - EXTRACT FACT COLUMNS

    # extract columns from joined song and log datasets to create songplays table 
    cond = [dfSong['title'] == df['song'], dfSong['artist_name'] == df['artist']]
    df = df.join(dfSong, cond)
    songplays_table = df[
        'start_time', 'userId', 'level', 'song_id', 'artist_id', 'sessionId', 'location', 'userAgent', 'date_year', 'date_month'
        ]
    songplays_table.select(monotonically_increasing_id().alias('songplay_id')).collect()



    ## STEP 4 - WRITE PARQUET FILES

    # write users table to parquet files
    users_table.write.parquet(output_data+'/user_table', 'overwrite')
    print('Completed: users_table written to S3')

    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(output_data+'/time_table',
                             mode='overwrite',
                             partitionBy=['date_year', 'date_month'])
    print('Completed: time_table written to S3')

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('date_year', 'date_month').parquet(os.path.join(output_data, 'songplays.parquet'), 'overwrite')

    print('\n\nCompleted: log data processed!\n\n')



def main():
    print('\n\n\nSparkify Data Lake ETL Process Started...\n\n\n')
    spark = create_spark_session()
    
    input_data = 's3a://udacity-dend/'
    output_data = 's3a://sparkifydatalake231/'
     

    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)

    print('\n\n\nSparkify Data Lake ETL Process Complete!\n\n\n')


if __name__ == '__main__':
    main()
