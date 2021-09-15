import configparser
import os
import json
import glob
import psycopg2
import pandas as pd
import boto3
import boto
from boto.s3.connection import S3Connection
from sql_queries import copy_table_queries, insert_dim_table_queries, insert_fact_table_queries
import awscli
import urllib
from sql_queries import time_table_insert, time_table_create, time_table_drop


run_stg_load = False
run_time_dim_load = False
run_dims_load = True
run_fact_load = True


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
LOG_DATA=config.get("S3","LOG_DATA")
LOG_JSONPATH=config.get("S3","LOG_JSONPATH")
SONG_DATA=config.get("S3","SONG_DATA")
DWH_ROLE_ARN = config.get("IAM_ROLE","ARN")


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_dim_tables(cur, conn):
    for query in insert_dim_table_queries:
        print("Starting query insert: {}".format(query))
        cur.execute(query)
        conn.commit()

def insert_fact_table(cur, conn):
    for query in insert_fact_table_queries:
        print("Starting query insert: {}".format(query))
        cur.execute(query)
        conn.commit()
    

def staging_events_copy_func(cur, conn, files, num_log_files):
    staging_events_copy = ""
    count = 0
    print('Starting Log Files Copy')
    for file in files:
        staging_events_copy = ("""copy events_log_stg 
                                from 's3://udacity-dend/{}'
                                iam_role '{}'
                                region 'us-west-2'
                                JSON '{}';
                                """).format(file, DWH_ROLE_ARN,LOG_JSONPATH)
        count += 1
        print('executing query: {} of {}'.format(count, num_log_files))
        cur.execute(staging_events_copy)
        print('executed query: {} of {}'.format(count, num_log_files))
        conn.commit()


def staging_songs_copy_func(cur, conn, files, num_song_files):
    staging_songs_copy = ""
    count = 0
    print('Starting Song Files Copy')
    for file in files: 
        staging_songs_copy = ("""
                                copy songs_log_stg 
                                from 's3://udacity-dend/{}'
                                iam_role '{}'
                                region 'us-west-2'
                                format as json 'auto' ;
                                """).format(file, DWH_ROLE_ARN)
        count += 1
        print('executing query: {} of {}'.format(count, num_song_files))
        cur.execute(staging_songs_copy)
        print('executed query: {} of {}'.format(count, num_song_files))
        conn.commit()

# def process_dims(cur, conn):
    #     songs_select = """
    #         SELECT song_id, artist_id, title, year, duration
    #         FROM songs_log_stg
    #     """
    # songs_df = pd.read_sql_table()

def process_data(cur, conn, log, song):
    helper = config.read_file(open('admin.cfg'))
    KEY = config.get('AWS','KEY')
    SECRET = config.get('AWS','SECRET')

    s3 = boto3.resource('s3',
                        region_name="us-west-2",
                         aws_access_key_id=KEY,
                         aws_secret_access_key=SECRET)
    
    s3bucket = s3.Bucket("udacity-dend")
    
    song_files = []
    
    if(song):
        count = 0
        print('Starting Song Files Read')
        for x in s3bucket.objects.filter(Prefix='song_'):
            if(x.key != 'song_data/'):
                count += 1
                print('processed song file {}'.format(count))
                song_files.append(x.key)
    
    log_files = []
    
    if(log):
        count = 0
        print('Starting Log Files Read')
        for y in s3bucket.objects.filter(Prefix='log_'):
            if(y.key != 'log_data/'):
                count += 1
                print('processed log file {}'.format(count))
                log_files.append(y.key)
    
    
    num_song_files = len(song_files)
    num_log_files = len(log_files)
    
    if(song):
        print(num_song_files)
        print(song_files[0])
    
    if(log):
        print(num_log_files)
        print(log_files[0])
    files = {'song_files': song_files,
            'log_files': log_files,
            'num_song_files': num_song_files,
            'num_log_files': num_log_files}
    
    return files

def process_time_dim(cur, conn):
    # drop time table if exists
    cur.execute(time_table_drop)
    
    # create time table if it doesn't exists
    cur.execute(time_table_create)
    conn.commit()
    
    # start time table load and parse
    df = pd.read_sql_query("SELECT ts FROM events_log_stg WHERE page='NextSong' ;", conn) #  
    t = df
    t['start_time'] = pd.to_datetime(t['ts'])
    
    date_dim = {}

    # take each datetime category from the original timestamp value 
    # and create a list out of all the different time-based categorical values
    for i in t.index.tolist():
        try: date_dim[i] = [t['ts'][i], t['start_time'][i].isoformat(), t['start_time'].dt.hour[i], t['start_time'].dt.day[i], t['start_time'].dt.week[i], 
                t['start_time'].dt.month[i], t['start_time'].dt.year[i], t['start_time'].dt.weekday[i]]
        except: print("Error at time extraction index: " + i)

    time_data = date_dim
    column_labels = ['time_key', 'start_time','hour','day','week', 'month', 'year','weekday']

    # initialize a time dataframe from the extracted data-based values 
    # so that we can iter over the rows in the dataframe when inserting into the time table
    time_df = pd.DataFrame.from_dict(date_dim, orient='index', columns=column_labels)
    
    # iterate over rows and insert date-based data into the time table
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))
    conn.commit()

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    # load_staging_tables(cur, conn)
    # insert_tables(cur, conn)
    if(run_stg_load):
        files = process_data(cur, conn, False, False)
        staging_events_copy_func(cur, conn, files['log_files'], files['num_log_files'])
        staging_songs_copy_func(cur, conn, files['song_files'], files['num_song_files'])
    elif(run_time_dim_load):
        print("Starting time dimension Insert")
        process_time_dim(cur, conn)
    
    if(run_dims_load):
        print("Starting dimension Inserts")
        insert_dim_tables(cur, conn)
    
    if(run_fact_load):
        print("Starting fact Inserts")
        insert_fact_table(cur, conn)
    
    conn.close()


if __name__ == "__main__":
    main()