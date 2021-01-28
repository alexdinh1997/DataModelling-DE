# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
### songplays tables is fact tables
songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays(
    songplay_id SERIAL PRIMARY KEY, 
    start_time bigint NOT NULL, 
    user_id int NOT NULL, 
    level varchar, 
    song_id varchar, 
    artist_id varchar, 
    session_id int, 
    location text, 
    user_agent text)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(
    user_id int PRIMARY KEY, 
    first_name varchar, 
    last_name varchar, 
    gender varchar(1), 
    level varchar)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(
    song_id varchar PRIMARY KEY, 
    title text, 
    artist_id varchar, 
    year int, 
    duration numeric)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists(
    artist_id varchar PRIMARY KEY, 
    name varchar, 
    location text, 
    latitude decimal, 
    longitude decimal)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time(
    start_time bigint PRIMARY KEY, 
    hour int, 
    day int, 
    week int, 
    month int, 
    year int, 
    weekday varchar)
""")

# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)\
VALUES(%s,%s,%s,%s,%s,%s,%s,%s)
""")
#update level for existing records when there is a conflict, but your code says "DO NOTHING".
user_table_insert = ("""
 INSERT INTO users(user_id, first_name, last_name, gender, level) \
 VALUES(%s,%s,%s,%s,%s)
 ON CONFLICT (user_id) DO UPDATE SET level=EXCLUDED.level
 """)

song_table_insert = ("""
INSERT INTO songs(song_id, title, artist_id, year, duration) \
VALUES(%s,%s,%s,%s,%s)
on conflict(song_id) do nothing
""")

artist_table_insert = ("""
INSERT INTO artists(artist_id, name, location, latitude, longitude) \
VALUES(%s,%s,%s,%s,%s)
on conflict(artist_id) do nothing
 """)


time_table_insert = ("""
INSERT INTO time(start_time, hour, day, week, month, year, weekday) \
VALUES(%s,%s,%s,%s,%s,%s,%s)
on conflict(start_time) do nothing
 """)

# FIND SONGS
# This one is a little more complicated since information from the songs table, artists table, and original log file are all needed for the songplays table. 
#Since the log file does not specify an ID for either the song or the artist,
#you'll need to get the song ID and artist ID by querying the songs and artists tables to find matches based on song title, artist name, and song duration time.

#Implement the song_select query in sql_queries.py to find the song ID and artist ID based on the title, artist name, and duration of a song.
#Select the timestamp, user ID, level, song ID, artist ID, session ID, location, and user agent and set to songplay_data

song_select = ("""
SELECT S.song_id, A.artist_id  \
FROM artists A JOIN songs S ON S.artist_id = A.artist_id \
WHERE S.title = %s AND A.name=%s AND S.duration =%s ;
"""
)

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
