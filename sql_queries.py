import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN             = config.get('IAM_ROLE', 'ARN')
LOG_DATA        = config.get('S3', 'LOG_DATA')
LOG_JSONPATH    = config.get('S3', 'LOG_JSONPATH')
SONG_DATA       = config.get('S3', 'SONG_DATA')
#$SONGS_JSONPATH  = config.get('S3', 'SONGS_JSONPATH')

# DROP TABLES

staging_events_table_drop = "drop TABLE IF EXISTS staging_events"
staging_songs_table_drop = "drop TABLE IF EXISTS staging_songs"
songplay_table_drop = "drop TABLE IF EXISTS songplays"
user_table_drop = "drop TABLE IF EXISTS users"
song_table_drop = "drop TABLE IF EXISTS songs"
artist_table_drop = "drop TABLE IF EXISTS artists"
time_table_drop = "drop TABLE IF EXISTS time"

# CREATE TABLES Staging 

staging_events_table_create= ("""
                    CREATE TABLE staging_events(
                                    event_id INT IDENTITY(0,1) NOT NULL,
                                    artist_name VARCHAR(255) ,
                                    auth VARCHAR(50) ,
                                    user_first_name VARCHAR(255) ,
                                    user_gender  VARCHAR(1) ,
                                    item_in_session	INTEGER,
                                    user_last_name VARCHAR(255) ,
                                    song_length	DOUBLE PRECISION, 
                                    user_level VARCHAR(50),
                                    location VARCHAR(255),	
                                    method VARCHAR(25),
                                    page VARCHAR(35),	
                                    registration VARCHAR(50),	
                                    session_id	BIGINT not null SORTKEY DISTKEY,
                                    song_title VARCHAR(255),
                                    status INTEGER, 
                                    ts VARCHAR(50),
                                    user_agent TEXT,	
                                    user_id integer) 
                              """)

staging_songs_table_create = ("""
                    CREATE TABLE staging_songs(
                                    num_songs INTEGER,
                                    artist_id VARCHAR(100) SORTKEY DISTKEY,
                                    artist_latitude DOUBLE PRECISION,
                                    artist_longitude DOUBLE PRECISION,
                                    artist_location VARCHAR(255),
                                    artist_name VARCHAR(255),
                                    song_id     VARCHAR(255) NOT NULL,
                                    title VARCHAR(255),
                                    duration DOUBLE PRECISION,
                                    year INTEGER )
                                """)

# CREATE TABLES Analytics

songplay_table_create = ("""
                        CREATE TABLE songplays(
                                    songplay_id INTEGER IDENTITY(0,1) sortkey,
                                    start_time TIMESTAMP,
                                    user_id VARCHAR(50) distkey,
                                    level VARCHAR(50),
                                    song_id VARCHAR(100),
                                    artist_id VARCHAR(100),
                                    session_id BIGINT,
                                    location VARCHAR(255),
                                    user_agent TEXT,
                                    PRIMARY KEY (songplay_id))
                        """)

user_table_create = ("""
                        CREATE TABLE users(
                                    user_id INTEGER  not null sortkey,
                                    first_name VARCHAR(255),
                                    last_name VARCHAR(255),
                                    gender VARCHAR(1),
                                    level VARCHAR(50),
                                    PRIMARY KEY (user_id)) diststyle all;
                        
                    """)


song_table_create = ("""
                        CREATE TABLE songs(
                                    song_id VARCHAR(100) NOT NULL SORTKEY,
                                    title VARCHAR(255),
                                    artist_id VARCHAR(100) NOT NULL,
                                    year INTEGER,
                                    duration DOUBLE PRECISION,
                                    PRIMARY KEY (song_id)) diststyle all;
                    """)

artist_table_create = ("""

                        CREATE TABLE artists(
                                    artist_id VARCHAR(100) NOT NULL SORTKEY,
                                    name VARCHAR(255),
                                    location VARCHAR(255),
                                    latitude DOUBLE PRECISION,
                                    longitude DOUBLE PRECISION,
                                    PRIMARY KEY (artist_id)) diststyle all;
                        """)

time_table_create = ("""
                        CREATE TABLE time(
                                start_time TIMESTAMP  NOT NULL SORTKEY,
                                hour INTEGER,
                                day INTEGER,
                                week INTEGER,
                                month INTEGER,
                                year INTEGER,
                                weekday INTEGER,
                                PRIMARY KEY (start_time)) diststyle all;
""")

# Copy Command for loading data in the staging

staging_events_copy = (""" copy staging_events 
                            from {} 
                            credentials 'aws_iam_role={}'
                            format as json {}
                            STATUPDATE ON
                            region 'us-west-2';
                            """).format(LOG_DATA,ARN,LOG_JSONPATH)

staging_songs_copy = (""" copy staging_songs
                            from {}
                            credentials 'aws_iam_role={}'
                            format as json 'auto'
                            ACCEPTINVCHARS AS '^'
                            STATUPDATE ON
                            region 'us-west-2';
                            """).format(SONG_DATA,ARN)


# Analytical table insert command

songplay_table_insert = ("""
                                INSERT INTO songplays ( 
                                        start_time,
                                        user_id,
                                        level,
                                        song_id,
                                        artist_id,
                                        session_id,
                                        location,
                                        user_agent)
                                            SELECT  DISTINCT TIMESTAMP 'epoch' + se.ts/1000 \
                                                        * INTERVAL '1 second'   AS start_time,
                                            se.user_id                   AS user_id,
                                            se.user_level                    AS level,
                                            ss.song_id                  AS song_id,
                                            ss.artist_id                AS artist_id,
                                            se.session_id                AS session_id,
                                            se.location                 AS location,
                                            se.user_agent                AS user_agent
                                            FROM staging_events AS se
                                                JOIN staging_songs AS ss
                                                    ON (se.artist_name = ss.artist_name)
                                            WHERE se.page = 'NextSong';
                                """)

user_table_insert = ("""
                                INSERT INTO users (                
                                        user_id,
                                        first_name,
                                        last_name,
                                        gender,
                                        level)
                                        SELECT  
                                        DISTINCT se.user_id          AS user_id,
                                        se.user_first_name                AS first_name,
                                        se.user_last_name                 AS last_name,
                                        se.user_gender                   AS gender,
                                        se.user_level                    AS level
                                        FROM staging_events AS se
                                        WHERE se.page = 'NextSong';
                        """)


song_table_insert = ("""
                                INSERT INTO songs (              
                                        song_id,
                                        title,
                                        artist_id,
                                        year,
                                        duration)
                                        SELECT 
                                            DISTINCT ss.song_id         AS song_id,
                                            ss.title                    AS title,
                                            ss.artist_id                AS artist_id,
                                            ss.year                     AS year,
                                            ss.duration                 AS duration
                                            FROM staging_songs AS ss;
                        """)

artist_table_insert = ("""
                                INSERT INTO artists (         
                                        artist_id,
                                        name,
                                        location,
                                        latitude,
                                        longitude)
                                        SELECT 
                                            DISTINCT ss.artist_id       AS artist_id,
                                            ss.artist_name              AS name,
                                            ss.artist_location          AS location,
                                            ss.artist_latitude          AS latitude,
                                            ss.artist_longitude         AS longitude
                                        FROM staging_songs AS ss;
                    """)

time_table_insert = ("""
                                 INSERT INTO time (                 
                                        start_time,
                                        hour,
                                        day,
                                        week,
                                        month,
                                        year,
                                        weekday)
                                        SELECT  DISTINCT TIMESTAMP 'epoch' + se.ts/1000 \
                                                        * INTERVAL '1 second'        AS start_time,
                                                EXTRACT(hour FROM start_time)    AS hour,
                                                EXTRACT(day FROM start_time)     AS day,
                                                EXTRACT(week FROM start_time)    AS week,
                                                EXTRACT(month FROM start_time)   AS month,
                                                EXTRACT(year FROM start_time)    AS year,
                                                EXTRACT(week FROM start_time)    AS weekday
                                        FROM    staging_events                   AS se
                                        WHERE se.page = 'NextSong';
                        """)

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
