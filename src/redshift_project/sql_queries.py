import configparser

# Constants
STAGING_TABLE_NAME_EVENT = "staging_events"
STAGING_TABLE_NAME_SONG = "staging_songs"
TABLE_NAME_SONGPLAY = "songplays"
TABLE_NAME_USER = "users"
TABLE_NAME_SONG = "songs"
TABLE_NAME_ARTIST = "artists"
TABLE_NAME_TIME = "time_table"

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = f"DROP TABLE IF EXISTS {STAGING_TABLE_NAME_EVENT}"
staging_songs_table_drop = f"DROP TABLE IF EXISTS {STAGING_TABLE_NAME_SONG}"
songplay_table_drop = f"DROP TABLE IF EXISTS {TABLE_NAME_SONGPLAY}"
user_table_drop = f"DROP TABLE IF EXISTS {TABLE_NAME_USER}"
song_table_drop = f"DROP TABLE IF EXISTS {TABLE_NAME_SONG}"
artist_table_drop = f"DROP TABLE IF EXISTS {TABLE_NAME_ARTIST}"
time_table_drop = f"DROP TABLE IF EXISTS {TABLE_NAME_TIME}"

# CREATE TABLES
# ts is in milliseconds

staging_events_table_create= f"""
    CREATE TABLE IF NOT EXISTS {STAGING_TABLE_NAME_EVENT}(
        artist              VARCHAR(MAX),
        auth                VARCHAR(MAX),
        firstName           VARCHAR(MAX),
        gender              VARCHAR(MAX),
        itemInSession       INT,
        lastName            VARCHAR(MAX),
        length              REAL,
        level               VARCHAR(MAX),
        location            VARCHAR(MAX),
        method              VARCHAR(MAX),
        page                VARCHAR(MAX),
        registration        TIMESTAMP,
        sessionId           INT,
        song                VARCHAR(MAX),
        status              INT,
        ts                  TIMESTAMP,
        userAgent           VARCHAR(MAX),
        userId              INT
    );
"""
# ?
# I don't know about `registration` What does it represent?
# song registration in the dataset?
# or user registration in the app?

staging_songs_table_create = f"""
    CREATE TABLE IF NOT EXISTS {STAGING_TABLE_NAME_SONG}(
        song_id             VARCHAR(MAX)     PRIMARY KEY,
        num_songs           INT,
        title               VARCHAR(MAX),
        artist_name         VARCHAR(MAX),
        artist_latitude     REAL,
        year                INT,
        duration            REAL,
        artist_id           VARCHAR(MAX),
        artist_longitude    REAL, 
        artist_location     VARCHAR(MAX)
    );
"""

songplay_table_create = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME_SONGPLAY}(
        songplay_id         INT             PRIMARY KEY,
        start_time          TIMESTAMP,
        user_id             INT,
        level               VARCHAR(MAX),
        song_id             VARCHAR(MAX)     DISTKEY,
        artist_id           VARCHAR(MAX),
        session_id          INT,
        location            VARCHAR(MAX),
        user_agent          VARCHAR(MAX),

        FOREIGN KEY (start_time) REFERENCES {TABLE_NAME_TIME}(start_time),
        FOREIGN KEY (user_id) REFERENCES {TABLE_NAME_USER}(user_id),
        FOREIGN KEY (song_id) REFERENCES {TABLE_NAME_SONG}(song_id),
        FOREIGN KEY (artist_id) REFERENCES {TABLE_NAME_ARTIST}(artist_id)
    );
"""


user_table_create = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME_USER}(
        user_id             INT             PRIMARY KEY,
        first_name          VARCHAR(MAX),
        last_name           VARCHAR(MAX),
        gender              VARCHAR(MAX),
        level               VARCHAR(MAX)
    )
    DISTSTYLE AUTO;
"""

song_table_create = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME_SONG}(
        song_id             VARCHAR(MAX)     PRIMARY KEY     DISTKEY,
        title               VARCHAR(MAX),
        artist_id           VARCHAR(MAX),
        year                INT,
        duration            REAL,

        FOREIGN KEY (artist_id) REFERENCES {TABLE_NAME_ARTIST}(artist_id)
    );
"""

artist_table_create = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME_ARTIST}(
        artist_id           VARCHAR(MAX)     PRIMARY KEY,
        name                VARCHAR(MAX),
        location            VARCHAR(MAX),
        latitude            VARCHAR(MAX),
        longitude           VARCHAR(MAX)
    )
    DISTSTYLE AUTO;
"""

time_table_create = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME_TIME}(
        start_time          TIMESTAMP       PRIMARY KEY,
        hour                INT,
        day                 INT,
        week                INT,
        month               INT,
        year                INT,
        weekday             INT
    )
    DISTSTYLE AUTO;
"""

# STAGING TABLES

staging_events_copy = f"""
    COPY {STAGING_TABLE_NAME_EVENT} FROM {config["S3"]["LOG_DATA"]}
    CREDENTIALS 'aws_iam_role={config['IAM_ROLE']['ARN']}'
    REGION 'us-west-2'
    FORMAT AS JSON {config["S3"]["LOG_JSONPATH"]}
    TIMEFORMAT AS 'epochmillisecs';
"""

staging_songs_copy = f"""
    COPY {STAGING_TABLE_NAME_SONG} FROM {config["S3"]["SONG_DATA"]}
    CREDENTIALS 'aws_iam_role={config['IAM_ROLE']['ARN']}'
    REGION 'us-west-2'
    FORMAT AS JSON 'auto';
"""

# FINAL TABLES

songplay_table_insert = f"""
    INSERT INTO {TABLE_NAME_SONGPLAY} (start_time, user_id, 
                                        level, song_id, artist_id, session_id, location, user_agent)
    SELECT 
        logs.ts, 
        logs.userId, 
        logs.level, 
        songs.song_id, 
        songs.artist_id, 
        logs.sessionId, 
        logs.location, 
        logs.userAgent
    FROM {STAGING_TABLE_NAME_EVENT}     AS      logs
    JOIN {STAGING_TABLE_NAME_SONG}      AS      songs
    ON logs.song = songs.title
    WHERE logs.page = 'NextSong';
"""

user_table_insert = f"""
    INSERT INTO {TABLE_NAME_USER} (user_id, first_name, last_name, gender, level)
    SELECT userId, firstName, lastName, gender, level
    FROM(
        SELECT 
            LAST(userId)       AS      userId, 
            LAST(firstName)    As      firstName,
            LAST(lastName)     As      lastName, 
            LAST(gender)       As      gender,
            LAST(level)        As      level,
            LAST(ts)           AS      ts
        FROM {STAGING_TABLE_NAME_EVENT}
        GROUPBY user_id
        ORDERBY ts ASC)
"""

song_table_insert = f"""
    INSERT INTO {TABLE_NAME_SONG} (song_id, title, artist_id, year, duration)
    SELECT DISTINCT(song_id), title, artist_id, year, duration
    FROM {STAGING_TABLE_NAME_SONG};
"""

artist_table_insert = f"""
    INSERT INTO {TABLE_NAME_ARTIST} (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT(artist_id), artist_name, artist_location, artist_latitude, artist_longitude
    FROM {STAGING_TABLE_NAME_SONG};
"""

time_table_insert = f"""
    INSERT INTO {TABLE_NAME_TIME} (start_time, hour, day, week, month, year, weekday)
    SELECT 
        DISTINCT(ts), 
        EXTRACT(hour FROM ts), 
        EXTRACT(day FROM ts), 
        EXTRACT(week FROM ts), 
        EXTRACT(month FROM ts), 
        EXTRACT(year FROM ts), 
        EXTRACT(weekday FROM ts), 
        EXTRACT(year FROM ts), 
        EXTRACT(weekday FROM ts)
    FROM {STAGING_TABLE_NAME_EVENT};
"""

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, time_table_create, user_table_create, artist_table_create, song_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
