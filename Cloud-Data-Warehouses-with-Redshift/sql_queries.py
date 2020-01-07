import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")
ARN = config.get("IAM_ROLE", "ARN")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_events
(
artist          VARCHAR,
auth            VARCHAR,
firstName       VARCHAR,
gender          VARCHAR,
itemInSession   INT,
lastName        VARCHAR,
length          NUMERIC,
level           VARCHAR,
location        VARCHAR,
method          VARCHAR,
page            VARCHAR,
registration    BIGINT,
/*contains over 12 digits, therefore needs BIGINT with 8 bytes
(8 * 8 = 64 bits) which can express up to 2^64/2-1*/
sessionId       INT,
song            VARCHAR,
status          INT,
ts              TIMESTAMP,
userAgent       VARCHAR,
userId          INT
)
""")


staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs
(
num_songs        INT,
artist_id        VARCHAR,
artist_latitude  NUMERIC,
artist_longitude NUMERIC,
artist_location  VARCHAR,
artist_name      VARCHAR,
song_id          VARCHAR,
title            VARCHAR,
duration         NUMERIC,
year             INT
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays
(
songplay_id       INT IDENTITY(0,1) PRIMARY KEY,
start_time        TIMESTAMP NOT NULL,
user_id           INT       NOT NULL,
level             VARCHAR,
song_id           VARCHAR,
artist_id         VARCHAR,
session_id        INT       NOT NULL,
location          VARCHAR,
user_agent        VARCHAR
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users
(
user_id    INT     PRIMARY KEY,
first_name VARCHAR NOT NULL,
last_name  VARCHAR,
gender     VARCHAR,
level      VARCHAR
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs
(
song_id   VARCHAR PRIMARY KEY,
title     VARCHAR NOT NULL,
artist_id VARCHAR,
year      INT,
duration  NUMERIC NOT NULL
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists
(
artist_id VARCHAR PRIMARY KEY,
name      VARCHAR NOT NULL,
location  VARCHAR,
latitude  NUMERIC,
longitude NUMERIC
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
(
start_time TIMESTAMP PRIMARY KEY,
hour       INT NOT NULL,
day        INT NOT NULL,
week       INT NOT NULL,
month      INT NOT NULL,
year       INT NOT NULL,
weekday    INT NOT NULL
)
""")

# STAGING TABLES
# Column names in Amazon Redshift tables are always lowercase,
# so when you use the ‘auto’ option, matching JSON field names
# must also be lowercase. If the JSON field name keys aren't all
# lowercase, you can use a JSONPaths file to explicitly map column
# names to JSON field name keys.
staging_events_copy = ("""
    COPY staging_events
    FROM {}
    CREDENTIALS 'aws_iam_role={}'
    REGION AS 'us-west-2'
    FORMAT AS JSON {}
    COMPUPDATE OFF
    TIMEFORMAT AS 'epochmillisecs'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    COPY staging_songs
    FROM {}
    CREDENTIALS 'aws_iam_role={}'
    REGION AS 'us-west-2'
    COMPUPDATE OFF
    FORMAT AS JSON 'auto'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO
   songplays (start_time, user_id, level, song_id, artist_id, session_id,
      location, user_agent)
   SELECT DISTINCT
      se.ts        AS start_time,
      se.userId    AS user_id,
      se.level     AS level,
      ss.song_id   AS song_id,
      ss.artist_id AS artist_id,
      se.sessionId AS session_id,
      se.location  AS location,
      se.userAgent AS user_agent
   FROM
      staging_events se
      JOIN
         staging_songs ss
         ON se.song = ss.title
         AND se.artist = ss.artist_name

""")

user_table_insert = ("""
INSERT INTO
   users(user_id, first_name, last_name, gender, level)
   SELECT DISTINCT
      userId    AS user_id,
      firstName AS first_name,
      lastName  AS last_name,
      gender    AS gender,
      level     AS level
   FROM
      staging_events
   WHERE userId IS NOT NULL
""")

song_table_insert = ("""
INSERT INTO
   songs(song_id, title, artist_id, year, duration)
   SELECT DISTINCT
      song_id   AS song_id,
      title     AS title,
      artist_id AS artist_id,
      year      AS year,
      duration  AS duration
   FROM
      staging_songs
   WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
INSERT INTO
   artists(artist_id, name, location, latitude, longitude)
   SELECT DISTINCT
      artist_id        AS artist_id,
      artist_name      AS name,
      artist_location  AS location,
      artist_latitude  AS latitude,
      artist_longitude AS longitude
   FROM
      staging_songs
   WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""
INSERT INTO
   time(start_time, hour, day, week, month, year, weekday)
   SELECT DISTINCT
      ts,
      EXTRACT(hour
   from
      ts),
      EXTRACT(day
   from
      ts),
      EXTRACT(week
   from
      ts),
      EXTRACT(month
   from
      ts),
      EXTRACT(year
   from
      ts),
      EXTRACT(weekday
   from
      ts)
   FROM
      staging_events
   WHERE ts IS NOT NULL
""")

# QUERY LISTS

create_table_queries = [
   staging_events_table_create, staging_songs_table_create,
   songplay_table_create, user_table_create, song_table_create,
   artist_table_create, time_table_create
]
drop_table_queries = [
   staging_events_table_drop, staging_songs_table_drop, songplay_table_drop,
   user_table_drop, song_table_drop, artist_table_drop, time_table_drop
]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [
   songplay_table_insert, user_table_insert, song_table_insert,
   artist_table_insert, time_table_insert
]
