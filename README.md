# This Repo Contians scripts and files for the Udacity Nanodegree Data Engineering AWS DW Project

## Introduction

### A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

### As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to.

## Process

### First open create_iam_and_redshift.ipynb to ensure redshift cluster is available

### Next run create_tables.py to DROP any previously created tables and Create the necessary staging, dim, and fact tables.

### Next run etl.py which goes through several stages:

#### The script first processes the events_log and songs_log files stored in the udacity-dend s3 bucket.

#### Next the script passes the files list to two functions:
##### staging_events_copy_func() and staging_songs_copy_func
#### Where both functions take in the events_log_files list and songs_log_files list respectively and copies the files to each table.

#### Next the script parses the ts attribute in events_log_stg and gets various date information from the file; such as, hour, day, week, month via the process_time_dim() function.  After parsing the various date information is inserted into the time dimension.

#### Afterwards, insert_dim_tables() function is run to insert data into the users dim, artists dim, and songs dim.

#### Finally, insert_fact_table() function is run to insert data into the fact table from the staging tables and various dimensions.

###  **** IMPORTANT NOTE ****
#### If you have already processed the data and created/inserted the tables then set the bool flags accordingly at the top of etl.py

## Analysis

### Check out the sql_queries.py file for drop, create, and insert functions.
### Check out the insert_queries_test.ipynb notebook to see the insert queries functions tested with staging data
### Check out the analysis.ipynb notebook for analysis queries.

## Run-Time

### If ran from beginning the process takes about 2-3 hours to complete due to a large number of song files and complex time parsing process.

