import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('/Users/abbas/Documents/udacity/mykeys/datawarehouseproject/dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_table;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay_table;"
user_table_drop = "DROP TABLE IF EXISTS user_table_drop;"
song_table_drop = "DROP TABLE IF EXISTS song_table_drop;"
artist_table_drop = "DROP TABLE IF EXISTS artist_table_drop;"
time_table_drop = "DROP TABLE IF EXISTS time_table_drop;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events(
    artist  varchar,
    auth varchar,
    firstName varchar,
    gender varchar,
    itemInSession int,
    lastName varchar,
    length numeric,
    level varchar,
    location varchar,
    method varchar,
    page varchar,
    registration numeric,
    sessionId int,
    song varchar,
    status int,
    ts bigint,
    userAgent text,
    userId int
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        artist_id varchar,
        artist_latitude float,
        artist_location varchar,
        artist_longitude float,
        artist_name varchar,
        duration float,
        num_songs int,
        song_id varchar,
        title varchar,
        year int
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id integer IDENTITY(1, 1) PRIMARY KEY , 
        start_time timestamp,
        user_id int, 
        level varchar,
        song_id varchar NOT NULL, 
        artist_id varchar NOT NULL,
        session_id int,
        location varchar, 
        user_agent varchar
    )
    DISTSTYLE KEY
    DISTKEY (start_time)
    SORTKEY (start_time);
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users(
        user_id int PRIMARY KEY,
        first_name varchar,
        last_name varchar,
        gender varchar,
        level varchar
    )
    SORTKEY (user_id);
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs(
        song_id varchar PRIMARY KEY,
        title varchar,
        artist_id varchar NOT NULL,
        year int,
        duration float
        )
    SORTKEY (song_id);
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists(
        artist_id varchar PRIMARY KEY,
        name varchar,
        location varchar,
        latitude numeric,
        longitude numeric
    )
    SORTKEY (artist_id);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time timestamp PRIMARY KEY,
    hour int,
    day int,
    week int,
    month int,
    year int,
    weekday int
    )

    DISTSTYLE KEY
    DISTKEY (start_time)
    SORTKEY (start_time);
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events 
FROM {0}
iam_role {1}
FORMAT AS json {2};
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
COPY staging_songs
FROM {0}
iam_role {1}
FORMAT AS json 'auto';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

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
    
    SELECT DISTINCT 
    TIMESTAMP 'epoch' + (ste.ts / 1000) * INTERVAL '1 second' as start_time,
                        ste.userId,
                        ste.level,
                        ste.song_id,
                        ste.artist_id,
                        ste.session_id,
                        ste.location,
                        ste.user_agent

    FROM  staging_songs sts
    INNER JOIN staging events ste
    ON (sts.title = ste.song AND sts.artist_name = ste.artist)
    AND ste.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users
SELECT DISTINCT userId, firstName, lastName, gender, level
FROM staging_events
where userId IS NOT NULL
AND page = 'NextSong';
""")

song_table_insert = ("""
INSERT INTO songs
SELECT DISTINCT  song_id, title, artist_id, year, duration
FROM staging_songs
WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO artists
SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude 
FROM staging_songs;
""")

time_table_insert = ("""
INSERT INTO time
SELECT DISTINCT
    TIMESTAMP 'epoch' + (ts/1000) * INTERVAL '1 second' as start_time,
    EXTRACT(HOUR FROM start_time) AS hour,
    EXTRACT(DAY FROM start_time) AS day,
    EXTRACT(WEEKS FROM start_time) AS week,
    EXTRACT(MONTH FROM start_time) AS month,
    EXTRACT(YEAR FROM start_time) AS year,
    to_char(start_time, 'Day') AS weekday
FROM staging_events;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
