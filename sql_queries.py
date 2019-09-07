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
    artist           VARCHAR           ENCODE ZSTD,
    auth             VARCHAR           ENCODE ZSTD,
    first_name       VARCHAR           ENCODE ZSTD,
    gender           VARCHAR           ENCODE ZSTD,
    item_in_session  INTEGER           ENCODE ZSTD,
    last_name        VARCHAR           ENCODE ZSTD,
    length           DOUBLE PRECISION  ENCODE ZSTD,
    level            VARCHAR           ENCODE ZSTD,
    location         VARCHAR           ENCODE ZSTD,
    method           VARCHAR           ENCODE ZSTD,
    page             VARCHAR           ENCODE ZSTD,
    registration     DOUBLE PRECISION  ENCODE ZSTD,
    session_id       INTEGER           ENCODE ZSTD,
    song             VARCHAR           ENCODE ZSTD,
    status           VARCHAR           ENCODE ZSTD,
    ts               BIGINT            ENCODE ZSTD,
    user_agent       VARCHAR           ENCODE ZSTD,
    user_id          INTEGER           ENCODE ZSTD
);
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
);
"""

songplay_table_create = """
CREATE TABLE songplays(
  songplay_id  INTEGER  IDENTITY(0,1)  ENCODE ZSTD,
  start_time   TIMESTAMP               ENCODE DELTA32K,
  user_id      INTEGER                 ENCODE ZSTD,
  level        VARCHAR                 ENCODE ZSTD,
  song_id      VARCHAR                 ENCODE ZSTD,
  artist_id    VARCHAR                 ENCODE ZSTD,
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
  song_id    VARCHAR           ENCODE ZSTD,
  title      VARCHAR           ENCODE ZSTD,
  artist_id  VARCHAR           ENCODE ZSTD,
  year       INTEGER           ENCODE ZSTD,
  duration   DOUBLE PRECISION  ENCODE ZSTD
);
"""

artist_table_create = """
CREATE TABLE artists (
    artist_id  VARCHAR           ENCODE ZSTD,
    name       VARCHAR           ENCODE ZSTD,
    location   VARCHAR           ENCODE ZSTD,
    latitude   DOUBLE PRECISION  ENCODE ZSTD,
    longitude  DOUBLE PRECISION  ENCODE ZSTD
);
"""

time_table_create = """
CREATE TABLE time(
    start_time  TIMESTAMP  ENCODE DELTA32K,
    hour        INTEGER    ENCODE ZSTD,
    day         INTEGER    ENCODE ZSTD,
    week        INTEGER    ENCODE ZSTD,
    month       INTEGER    ENCODE ZSTD,
    year        INTEGER    ENCODE ZSTD,
    weekday     INTEGER    ENCODE ZSTD
);
"""


# STAGING TABLES
def build_copy_sql(table, filepath, json_format):
    return f"""
    COPY {table}
    FROM {filepath}
    IAM_ROLE {IAM_ROLE}
    FORMAT AS JSON {json_format}
    """


staging_events_copy = build_copy_sql('events_stage', LOG_DATA, json_format=LOG_JSONPATH)

staging_songs_copy = build_copy_sql('songs_stage', SONG_DATA, json_format="'auto'")

# FINAL TABLES
# Load song data from staging table into songs
songs_load = """
    INSERT INTO songs
    SELECT DISTINCT
        song_id,
        title,
        artist_id,
        year,
        duration
    FROM songs_stage;
"""

# Load artist data from staging table into artists
artists_load = """
    INSERT INTO artists
    SELECT DISTINCT
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM songs_stage;
"""

# Load unique users from staging table into users
# For users with multiple levels, selects the most recent level
users_load = """
    INSERT INTO users
    SELECT DISTINCT
        user_id,
        first_name,
        last_name,
        gender,
        level
    FROM (
        SELECT
            ts,
            user_id,
            first_name,
            last_name,
            gender,
            level
        FROM events_stage
        ORDER BY ts DESC
    ) users_temp
"""

# Extract, convert, and split log timestamps to load into time table
time_load = """
    INSERT into TIME
     SELECT DISTINCT
       ts,
       extract(hour FROM ts) as hour,
       extract(day FROM ts) as day,
       extract(week FROM ts) as week,
       extract(month FROM ts) as month,
       extract(year FROM ts) as year,
       extract(dow FROM ts) as weekday
    FROM (
      SELECT timestamp 'epoch' + ts / 1000 * interval '1 second' AS ts
      FROM events_stage
    ) next_song_ts;
"""

# Load songplays data from staging table, joining on songs and artists
songplays_load = """
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT
        timestamp 'epoch' + e.ts / 1000 * interval '1 second' AS ts,
        e.user_id,
        e.level,
        s.song_id,
        a.artist_id,
        e.session_id,
        e.location,
        e.user_agent
    FROM events_stage e
    LEFT JOIN songs s ON s.title = e.song
    LEFT JOIN artists a ON a.name = e.artist
"""
# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create,
                        user_table_create, song_table_create, artist_table_create, time_table_create]
drop_tables_query = drop_table_sql
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplays_load, users_load, songs_load, artists_load, time_load]
