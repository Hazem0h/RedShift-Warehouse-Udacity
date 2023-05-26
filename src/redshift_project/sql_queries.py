import configparser

# Constants
STAGING_TABLE_NAME_EVENT = "staging_events"
STAGING_TABLE_NAME_SONG = "staging_songs"
TABLE_NAME_SONGPLAY = "songplays"
TABLE_NAME_USER = "users"
TABLE_NAME_SONG = "songs"
TABLE_NAME_ARTIST = "artists"
TABLE_NAME_TIME = "time"

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

staging_events_table_create= f"""
    CREATE TABLE IF NOT EXISTS {STAGING_TABLE_NAME_EVENT}(
        //event ID
        eventId             IDENTITY(0,1)

        // user data
        userId              INT,
        firstName           VARCHAR(50),
        lastName            VARCHAR(50),
        gender              VARCHAR(1),
        level               VARCHAR(50),

        // session data
        location            VARCHAR(50),
        userAgent           VARCHAR(50),
        auth                VARCHAR(50),
        sessionId           INT,

        // request info
        ts                  TIMESTAMP       SORTKEY, //because we want the latest timestamp to update the user level.
        itemInSession       INT,
        method              VARCHAR(50),
        status              INT,
        page                VARCHAR(50),

        // song data
        song                VARCHAR(50),
        artist              VARCHAR(50),
        length              REAL,
        registration        TIMESTAMP
    );
"""
# ?
# I don't know about `registration` What does it represent?
# song registration in the dataset?
# or user registration in the app?

staging_songs_table_create = f"""
    CREATE TABLE IF NOT EXISTS {STAGING_TABLE_NAME_SONG}(
        //song data
        song_id             VARCHAR(50)     PRIMARY KEY,
        num_songs           INT,
        title               VARCHAR(50),
        duration            REAL,
        year                INT,

        // artist data
        artist_id           VARCHAR(50),
        artist_latitude     VARCHAR(50), 
        artist_longitude    VARCHAR(50), 
        artist_location     VARCHAR(50), 
        artist_name         VARCHAR(50)
    );
"""

songplay_table_create = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME_SONGPLAY}(
        songplay_id         INT             PRIMARY KEY,
        start_time          TIMESTAMP,
        user_id             INT,
        level               VARCHAR(50),
        song_id             VARCHAR(50)     DISTKEY,
        artist_id           VARCHAR(50),
        session_id          INT,
        location            VARCHAR(50),
        user_agent          VARCHAR(50),

        FOREIGN KEY (start_time) REFERENCES {TABLE_NAME_TIME}(start_time),
        FOREIGN KEY (user_id) REFERENCES {TABLE_NAME_USER}(user_id),
        FOREIGN KEY (song_id) REFERENCES {TABLE_NAME_SONG}(song_id),
        FOREIGN KEY (artist_id) REFERENCES {TABLE_NAME_ARTIST}(artist_id)
    );
"""


user_table_create = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME_USER}(
        user_id             INT             PRIMARY KEY,
        first_name          VARCHAR(50),
        last_name           VARCHAR(50),
        gender              VARCHAR(1),
        level               VARCHAR(50)

        DISTSTYLE AUTO
    );
"""

song_table_create = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME_SONG}(
        song_id             VARCHAR(50)     PRIMARY KEY     DISTKEY,
        title               VARCHAR(50),
        artist_id           VARCHAR(50),
        year                INT,
        duration            REAL,

        FOREIGN KEY (artist_id) REFERENCES {TABLE_NAME_ARTIST}(artist_id)
    );
"""

artist_table_create = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME_ARTIST}(
        artist_id           VARCHAR(50)     PRIMARY KEY     DISTKEY,
        name                VARCHAR(50),
        location            VARCHAR(50),
        latitude            VARCHAR(50),
        longitude           VARCHAR(50),

        DISTSTYLE AUTO
    );
"""

time_table_create = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME_TIME}(
        start_time          TIMESTAMP       PRIMARY KEY,
        hour                INT,
        day                 INT,
        week                INT,
        month               INT,
        year                INT,
        weekday             INT,

        DISTSTYLE AUTO
    );
"""

# STAGING TABLES

staging_events_copy = f"""
    COPY {STAGING_TABLE_NAME_EVENT} FROM {config["S3"]["LOG_DATA"]}
    CREDENTIALS {config['IAM_ROLE']['ARN']};
"""

staging_songs_copy = f"""
    COPY {STAGING_TABLE_NAME_SONG} FROM {config["S3"]["SONG_DATA"]}
    CREDENTIALS {config['IAM_ROLE']['ARN']};
"""

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
