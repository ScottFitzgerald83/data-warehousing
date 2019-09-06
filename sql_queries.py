import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS times"

# CREATE TABLES

staging_events_table_create = """
CREATE TABLE events_stage(
    artist         VARCHAR,
    auth           VARCHAR,
    firstName      VARCHAR,
    gender         VARCHAR,
    itemInSession  INTEGER,
    lastName       VARCHAR,
    length         DOUBLE PRECISION,
    level          VARCHAR,
    location       VARCHAR,
    method         VARCHAR,
    page           VARCHAR,
    registration   DOUBLE PRECISION,
    sessionId      INTEGER,
    song           VARCHAR,
    status         VARCHAR,
    ts             INTEGER              ,
    userAgent      VARCHAR,
    userId         INTEGER
)
"""

songs_table_create = """
CREATE TABLE songs_stage (
    artist_id         VARCHAR,
    artist_latitude   DOUBLE PRECISION,
    artist_location   VARCHAR,
    artist_longitude  DOUBLE PRECISION,
    artist_name       VARCHAR,
    duration          DOUBLE PRECISION,
    num_songs         INTEGER,
    song_id           VARCHAR,
    title             VARCHAR,
    year              INTEGER
)
"""

songplay_table_create = """
CREATE TABLE songplays(
  songplay_id  INTEGER  IDENTITY(0,1)  ENCODE ZSTD,
  start_time   TIMESTAMP               ENCODE DELTA32K,
  user_id      INTEGER                 ENCODE ZSTD,
  level        VARCHAR                 ENCODE ZSTD,
  song_id      INTEGER                 ENCODE ZSTD,
  artist_id    INTEGER                 ENCODE ZSTD,
  session_id   INTEGER                 ENCODE ZSTD,
  location     VARCHAR                 ENCODE ZSTD,
  user_agent   VARCHAR                 ENCODE ZSTD
);
"""

user_table_create = """
CREATE TABLE users (
  user_id     INTEGER  ENCODE ZSTD,
  first_name  VARCHAR  ENCODE ZSTD,
  last_name   VARCHAR  ENCODE ZSTD,
  gender      VARCHAR  ENCODE ZSTD,
  level       VARCHAR  ENCODE ZSTD
);
"""

song_table_create = """
CREATE TABLE songs(
  song_id    INTEGER           ENCODE ZSTD,
  title      VARCHAR           ENCODE ZSTD,
  artist_id  INTEGER           ENCODE ZSTD,
  year       INTEGER           ENCODE ZSTD,
  duration   DOUBLE PRECISION  ENCODE ZSTD
);
"""

artist_table_create = """
CREATE TABLE songs (
    song_id    INTEGER  NOT NULL,
    title      VARCHAR,
    artist_id  INTEGER  NOT NULL,
    year       INTEGER,
    duration   DOUBLE PRECISION
);
"""

time_table_create = """
CREATE TABLE TIME(
    start_time INTEGER,
    hour INTEGER, 
    day INTEGER, 
    week INTEGER, 
    month INTEGER, 
    year INTEGER, 
    weekday INTEGER
);

"""

# STAGING TABLES

staging_events_copy = ("""
""").format()

staging_songs_copy = ("""
""").format()

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
