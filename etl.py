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


run_stg_load = True # set to True when needing to run stage load, False if not
run_time_dim_load = True # set to True when needing to run time dim load, False if not
run_dims_load = True # set to True when needing to run non-time dim loads, False if not
run_fact_load = True # set to True when needing to run fact load, False if not


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
LOG_DATA=config.get("S3","LOG_DATA")
LOG_JSONPATH=config.get("S3","LOG_JSONPATH")
SONG_DATA=config.get("S3","SONG_DATA")
DWH_ROLE_ARN = config.get("IAM_ROLE","ARN")


def insert_dim_tables(cur, conn):
    """
    function to insert data from staging tables into:
    users dim, artists_dim, and songs_dim using queries in sql_queries.py
    Parameters:
        - cur = connection cursor
        - conn = aws postgres connection
    Outputs:
        None
    """

    for query in insert_dim_table_queries:
        print("Starting query insert: {}".format(query))
        cur.execute(query)
        conn.commit()

def insert_fact_table(cur, conn):
    """
    function to insert data from staging tables and dim's into fact table.
    insert statement is in sql_queries.py
    Parameters:
        - cur = connection cursor
        - conn = aws postgres connection
    Outputs:
        None
    """

    for query in insert_fact_table_queries:
        print("Starting query insert: {}".format(query))
        cur.execute(query)
        conn.commit()
    

def staging_events_copy_func(cur, conn, files, num_log_files):
    """
    function for implementing staging events insert 
    
    Parameters: 
        - cur = connection cursor
        - conn = aws postgres connection,
        - files = list of events files
        - num_log_files = number of log files
    Outputs: 
        None
    """

    staging_events_copy = ""
    count = 0
    print('Starting Log Files Copy')
    
    # for file in files list passed in that were processed in process_data()
    # iterate copy data from file into events_log_stg
    for file in files:
        staging_events_copy = ("""copy staging.events_log_stg 
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
    """
    function for implementing staging songs insert
    Parameters: 
        - cur = connection cursor
        - conn = aws postgres connection
        - files = list of events files
        - num_song_files = number of song files
    Outputs:
        None
    """
    
    staging_songs_copy = ""
    count = 0
    print('Starting Song Files Copy')
    # for file in files list passed in that were processed in process_data()
    # iterate copy data from file into songs_log_stg
    for file in files: 
        staging_songs_copy = ("""
                                copy staging.songs_log_stg 
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

def process_data(cur, conn, log, song):
    """
    function to process file names stored in udacity-dend bucket
    processes json files in bucket with prefixc song_ and log_
    
    Parameters: 
        - cur = connection cursor, conn = aws postgres connection,
        - log = boolean to determine whether to parse log files
    song = boolean to determine whether to parse song files
    
    Outputs: 
        - file names dictionary containing song and log files
    
    """
    
    # read config files to get key and secret to connect to s3 bucket
    helper = config.read_file(open('admin.cfg'))
    KEY = config.get('AWS','KEY')
    SECRET = config.get('AWS','SECRET')
    
    # s3 resource via boto3
    s3 = boto3.resource('s3',
                        region_name="us-west-2",
                         aws_access_key_id=KEY,
                         aws_secret_access_key=SECRET)
    
    s3bucket = s3.Bucket("udacity-dend")
    
    song_files = []
    
    # process song files
    if(song):
        count = 0
        print('Starting Song Files Read')
        for x in s3bucket.objects.filter(Prefix='song_'):
            if(x.key != 'song_data/'):
                count += 1
                print('processed song file {}'.format(count))
                song_files.append(x.key)
    
    log_files = []
    
    # process log files
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

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    # if run_stg_load is true then process log and song data in udacity-dend s3 bucket
    # then copy files from udacity-dend s3 bucket into songs_log_stg and events_log_stg
    if(run_stg_load):
        files = process_data(cur, conn, True, True)
        staging_events_copy_func(cur, conn, files['log_files'], files['num_log_files'])
        staging_songs_copy_func(cur, conn, files['song_files'], files['num_song_files'])
    
    # if run_dims_load is true then run insert statements for:
    # songs dim, artists dim, and users dim
    if(run_dims_load):
        print("Starting dimension Inserts")
        insert_dim_tables(cur, conn)
    
    # if run_fact_load is true then run insert statement for songplays fact table
    if(run_fact_load):
        print("Starting fact Inserts")
        insert_fact_table(cur, conn)
    
    conn.close()


if __name__ == "__main__":
    main()