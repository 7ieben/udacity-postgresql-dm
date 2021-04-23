# DROP TYPES

level_type_drop = "DROP TYPE IF EXISTS user_level"

# CREATE TYPES

level_type_create = "CREATE TYPE user_level AS ENUM ('free', 'paid')"

# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

songplay_table_create = ("CREATE TABLE IF NOT EXISTS songplays ( songplay_id serial PRIMARY KEY \
                                                               , start_time timestamp NOT NULL \
                                                               , user_id int NOT NULL \
                                                               , level user_level NOT NULL \
                                                               , song_id varchar \
                                                               , artist_id varchar \
                                                               , session_id int NOT NULL \
                                                               , location varchar \
                                                               , user_agent varchar \
                                                               , CONSTRAINT fk_start_time \
                                                                   FOREIGN KEY(start_time) \
                                                                     REFERENCES time(start_time) \
                                                               , CONSTRAINT fk_user_id \
                                                                   FOREIGN KEY(user_id) \
                                                                     REFERENCES users(user_id) \
                                                               , CONSTRAINT fk_artist_id \
                                                                   FOREIGN KEY(artist_id) \
                                                                     REFERENCES artists(artist_id) \
                                                               , CONSTRAINT fk_song_id \
                                                                   FOREIGN KEY(song_id) \
                                                                     REFERENCES songs(song_id));")

user_table_create = ("CREATE TABLE IF NOT EXISTS users ( user_id int PRIMARY KEY \
                                                       , first_name text \
                                                       , last_name text \
                                                       , gender char \
                                                       , level user_level NOT NULL);")

song_table_create = ("CREATE TABLE IF NOT EXISTS songs ( song_id varchar PRIMARY KEY \
                                                       , title varchar NOT NULL \
                                                       , artist_id varchar \
                                                       , year int \
                                                       , duration interval);")

artist_table_create = ("CREATE TABLE IF NOT EXISTS artists ( artist_id varchar PRIMARY KEY \
                                                           , name varchar NOT NULL \
                                                           , location text \
                                                           , latitude numeric \
                                                           , longitude numeric);")

time_table_create = ("CREATE TABLE IF NOT EXISTS time ( start_time timestamp PRIMARY KEY \
                                                      , hour int NOT NULL \
                                                      , day int NOT NULL \
                                                      , week int NOT NULL \
                                                      , month int NOT NULL \
                                                      , year int NOT NULL \
                                                      , weekday int NOT NULL);")

# INSERT RECORDS

songplay_table_insert = ("INSERT INTO songplays (start_time, user_id, level, song_id, \
                                                 artist_id, session_id, location, user_agent) \
                          VALUES (%s, %s, %s, %s, %s, %s, %s, %s) \
                          ON CONFLICT ON CONSTRAINT songplays_pkey \
                          DO NOTHING;")

user_table_insert = ("INSERT INTO users (user_id, first_name, last_name, gender, level) \
                      VALUES (%s, %s, %s, %s, %s) \
                      ON CONFLICT ON CONSTRAINT users_pkey \
                      DO UPDATE SET level=EXCLUDED.level;")

song_table_insert = ("INSERT INTO songs (song_id, title, artist_id, year, duration) \
                      VALUES (%s, %s, %s, %s, %s) \
                      ON CONFLICT ON CONSTRAINT songs_pkey \
                      DO NOTHING;")

artist_table_insert = ("INSERT INTO artists (artist_id, name, location, latitude, longitude) \
                        VALUES (%s, %s, %s, %s, %s) \
                        ON CONFLICT ON CONSTRAINT artists_pkey \
                        DO NOTHING;")

time_table_insert = ("INSERT INTO time (start_time, hour, day, week, month, year, weekday) \
                      VALUES (%s, %s, %s, %s, %s, %s, %s) \
                      ON CONFLICT ON CONSTRAINT time_pkey \
                      DO NOTHING;")

# FIND SONGS

song_select = ("SELECT songs.song_id, songs.artist_id \
                FROM songs JOIN artists ON songs.artist_id = artists.artist_id \
                WHERE songs.title = %s AND artists.name = %s AND songs.duration = %s;")

# QUERY LISTS

create_type_queries = [level_type_create]
drop_type_queries = [level_type_drop]
create_table_queries = [user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]