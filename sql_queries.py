import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
LOG_DATA=config.get("S3","LOG_DATA")
LOG_JSONPATH=config.get("S3","LOG_JSONPATH")
SONG_DATA=config.get("S3","SONG_DATA")
DWH_ROLE_ARN = config.get("IAM_ROLE","ARN")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS events_log_stg"
staging_songs_table_drop = "DROP TABLE IF EXISTS songs_log_stg"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS events_log_stg
(
artist VARCHAR(1000), auth VARCHAR(100), firstName VARCHAR(100), gender VARCHAR(10), itemInSession INT, lastName VARCHAR(100),
length FLOAT, level VARCHAR(100), location VARCHAR(200), method VARCHAR(10), page VARCHAR(100), registration VARCHAR(100),
sessionId INT, song VARCHAR(1000), status INT, ts BIGINT, userAgent VARCHAR(1000), userId VARCHAR(100)
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS songs_log_stg 
(
artist_id VARCHAR(1000), artist_latitude NUMERIC, 
artist_location VARCHAR(1000), artist_longitude NUMERIC, 
artist_name VARCHAR(1000), duration NUMERIC, num_songs INT, 
song_id VARCHAR(1000), title VARCHAR(1000), year int
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays
(start_time TIMESTAMP NOT NULL, time_key BIGINT, user_key INT,
level VARCHAR(100), song_key INT,
artist_key INT, session_id INT,
location VARCHAR(200), user_agent VARCHAR(1000),
CONSTRAINT fk_time FOREIGN KEY(time_key) REFERENCES time (time_key),
CONSTRAINT fk_users FOREIGN KEY(user_key) REFERENCES users (user_key),
CONSTRAINT fk_songs FOREIGN KEY(song_key) REFERENCES songs (song_key),
CONSTRAINT fk_artists FOREIGN KEY(artist_key) REFERENCES artists (artist_key))
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users
( user_key INT IDENTITY(0,1), user_id VARCHAR(30) UNIQUE NOT NULL,
first_name VARCHAR(20), last_name VARCHAR(20), gender CHAR(4),
level VARCHAR(100),
PRIMARY KEY (user_key)
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs
(song_key INT IDENTITY(0,1), song_id VARCHAR(1000) UNIQUE NOT NULL,
title VARCHAR(1000) NOT NULL, artist_key INT, year INT, duration NUMERIC,
PRIMARY KEY (song_key),
CONSTRAINT fk_artists FOREIGN KEY(artist_key) REFERENCES artists (artist_key)
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists
(artist_key INT IDENTITY(0,1), artist_id VARCHAR(1000) UNIQUE NOT NULL,
name VARCHAR(1000), location VARCHAR(1000), latitude NUMERIC, longitude NUMERIC,
PRIMARY KEY (artist_key)
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
(time_key BIGINT, start_time TIMESTAMP, hour INT, day INT,
week INT, month INT, year INT, weekday INT,
PRIMARY KEY (time_key)
)
""")

# STAGING TABLES

staging_events_copy = ("""
copy events_log_stg 
from '{}'
iam_role '{}'
region 'us-west-2'
JSON '{}';
""").format(LOG_DATA, DWH_ROLE_ARN, LOG_JSONPATH)

staging_songs_copy = """"""
helper = ("""
copy songs_log_stg 
from '{}'
iam_role {}
region 'us-west-2'
JSON 'songs_log_json_paths.json';
""").format(SONG_DATA, DWH_ROLE_ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time,
time_key, user_key, level, song_key, artist_key, 
session_id, location, user_agent)
SELECT time.start_time, time.time_key, users.user_key,
users.level, songs.song_key, artists.artist_key,
events_log_stg.sessionId as session_id,
events_log_stg.location, events_log_stg.userAgent
FROM events_log_stg
JOIN users
    ON events_log_stg.userId = users.user_id
JOIN time
    ON events_log_stg.ts = time.time_key
JOIN songs
    ON events_log_stg.song = songs.title
JOIN artists
    ON songs.artist_key = artists.artist_key
""")

user_table_insert = ("""
INSERT INTO users (user_id,
first_name, last_name, gender, level)
SELECT userId as user_id, firstName as first_name,
lastName as last_name, gender, level
FROM events_log_stg
WHERE userId IS NOT NULL
""")

song_table_insert = ("""
INSERT INTO songs (song_id,
title, artist_key, year, duration)
SELECT song_id, title, artist_key, year, duration
FROM songs_log_stg
JOIN artists
    ON artists.artist_id = songs_log_stg.artist_id
WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id,
name, location, latitude, longitude)
SELECT artist_id, artist_name as name, artist_location as location,
artist_latitude as latitude, artist_longitude as longitude
FROM songs_log_stg
WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""
INSERT INTO time (time_key, start_time, hour, day, week, month, year, weekday)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, artist_table_create, user_table_create, song_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_dim_table_queries = [user_table_insert, artist_table_insert, song_table_insert]
insert_fact_table_queries = [songplay_table_insert]
