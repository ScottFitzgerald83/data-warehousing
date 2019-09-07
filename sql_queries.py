import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

IAM_ROLE = config.get('IAM_ROLE', 'ARN')
LOG_DATA = config.get('S3', 'LOG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA = config.get('S3', 'SONG_DATA')

# DROP TABLES
drop_table_sql = 'DROP TABLE IF EXISTS events_stage, songs_stage, songplays, users, songs, artists, time'

# CREATE TABLES
staging_events_table_create = """
CREATE TABLE events_stage(
    artist         VARCHAR           ENCODE ZSTD,
    auth           VARCHAR           ENCODE ZSTD,
    firstName      VARCHAR           ENCODE ZSTD,
    gender         VARCHAR           ENCODE ZSTD,
    itemInSession  INTEGER           ENCODE ZSTD,
    lastName       VARCHAR           ENCODE ZSTD,
    length         DOUBLE PRECISION  ENCODE ZSTD,
    level          VARCHAR           ENCODE ZSTD,
    location       VARCHAR           ENCODE ZSTD,
    method         VARCHAR           ENCODE ZSTD,
    page           VARCHAR           ENCODE ZSTD,
    registration   DOUBLE PRECISION  ENCODE ZSTD,
    sessionId      INTEGER           ENCODE ZSTD,
    song           VARCHAR           ENCODE ZSTD,
    status         VARCHAR           ENCODE ZSTD,
    ts             BIGINT            ENCODE ZSTD,
    userAgent      VARCHAR           ENCODE ZSTD,
    userId         INTEGER           ENCODE ZSTD
)
"""

staging_songs_table_create = """
CREATE TABLE songs_stage (
    artist_id         VARCHAR           ENCODE ZSTD,
    artist_latitude   DOUBLE PRECISION  ENCODE ZSTD,
    artist_location   VARCHAR           ENCODE ZSTD,
    artist_longitude  DOUBLE PRECISION  ENCODE ZSTD,
    artist_name       VARCHAR           ENCODE ZSTD,
    duration          DOUBLE PRECISION  ENCODE ZSTD,
    num_songs         INTEGER           ENCODE ZSTD,
    song_id           VARCHAR           ENCODE ZSTD,
    title             VARCHAR           ENCODE ZSTD,
    year              INTEGER           ENCODE ZSTD
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
CREATE TABLE artists (
    song_id     INTEGER NOT NULL  ENCODE ZSTD, 
    title       VARCHAR           ENCODE ZSTD,
    artist_id   INTEGER           ENCODE ZSTD,
    year        INTEGER           ENCODE ZSTD,
    duration    DOUBLE PRECISION  ENCODE ZSTD
);
"""

time_table_create = """
CREATE TABLE TIME(
    start_time  INTEGER  ENCODE ZSTD,
    hour        INTEGER  ENCODE ZSTD,
    day         INTEGER  ENCODE ZSTD,
    week        INTEGER  ENCODE ZSTD,
    month       INTEGER  ENCODE ZSTD,
    year        INTEGER  ENCODE ZSTD,
    weekday     INTEGER  ENCODE ZSTD
);
"""


# STAGING TABLES
def build_copy_sql(table, filepath):
    return f"""
    COPY {table} 
    FROM {filepath}
    IAM_ROLE {IAM_ROLE}
    FORMAT AS JSON 'auto';
    """


staging_events_copy = build_copy_sql('events_stage', LOG_DATA)

staging_songs_copy = build_copy_sql('songs_stage', SONG_DATA)

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

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create,
                        user_table_create, song_table_create, artist_table_create, time_table_create]
drop_tables_query = drop_table_sql
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert,
                        time_table_insert]
