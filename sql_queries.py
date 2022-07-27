import configparser

config = configparser.ConfigParser()
config.read('dwh.cfg')

KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')
LOG_DATA               = config.get("S3","LOG_DATA")
LOG_PATH               = config.get("S3", "LOG_JSONPATH")
SONG_DATA              = config.get("S3", "SONG_DATA")


staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS fact_songplay"
user_table_drop = "DROP TABLE IF EXISTS dim_user"
song_table_drop = "DROP TABLE IF EXISTS dim_song"
artist_table_drop = "DROP TABLE IF EXISTS dim_artist"
time_table_drop = "DROP TABLE IF EXISTS dim_time"


staging_events_table_create=  ("""
    CREATE TABLE IF NOT EXISTS staging_events
    (
    artist          VARCHAR,
    auth            VARCHAR, 
    firstName       VARCHAR(255),
    gender          CHAR,   
    itemInSession   INTEGER,
    lastName        VARCHAR(255),
    length          FLOAT,
    level           VARCHAR(50), 
    location        VARCHAR(255),
    method          VARCHAR(255),
    page            VARCHAR(50),
    registration    BIGINT,
    sessionId       INTEGER,
    song            VARCHAR(255),
    status          INTEGER,
    ts              TIMESTAMP,
    userAgent       VARCHAR(50),
    userId          INTEGER
    );
    """)

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs
    (
    num_songs          INTEGER,
    title              VARCHAR,
    artist_name        VARCHAR,
    artist_latitude    FLOAT,
    year               INTEGER,
    duration           FLOAT,
    artist_id          VARCHAR,
    artist_longitude   FLOAT,
    artist_location    VARCHAR
    );
    """)

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS fact_songplay
    (
    songplay_id          NOT NULL INTEGER IDENTITY(0,1) PRIMARY KEY,
    start_time           NOT NULL TIMESTAMP,
    user_id              NOT NULL INTEGER,
    level                VARCHAR,
    song_id              NOT NULL VARCHAR,
    artist_id            NOT NULL VARCHAR,
    session_id           INTEGER,
    location             VARCHAR,
    user_agent           VARCHAR
    )
    SORTKEY (songplay_id);
    """)

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS dim_user
    (
    user_id         NOT NULL INTEGER PRIMARY KEY distkey,
    first_name      NOT NULL VARCHAR,
    last_name       NOT NULL VARCHAR,
    gender          VARCHAR,
    level           VARCHAR
    )
    SORTKEY (user_Id);
    """)

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS dim_song
    (
    song_id     NOT NULL VARCHAR PRIMARY KEY,
    title       NOT NULL VARCHAR,
    artist_id   NOT NULL VARCHAR distkey,
    year        INTEGER,
    duration    FLOAT
    )
    SORTKEY (song_id);
    """)

artist_table_create =  ("""
    CREATE TABLE IF NOT EXISTS dim_artist
    (
    artist_id          NOT NULL VARCHAR PRIMARY KEY distkey,
    name               NOT NULL VARCHAR,
    location           VARCHAR,
    latitude           FLOAT,
    longitude          FLOAT
    )
    SORTKEY (artist_id);
    """)

time_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_time
    (
    start_time    NOT NULL TIMESTAMP PRIMARY KEY distkey,
    hour          INTEGER,
    day           INTEGER,
    week          INTEGER,
    month         INTEGER,
    year          INTEGER,
    weekday       INTEGER
    )
    SORTKEY (start_time);
    """)


staging_events_copy = ("""
    COPY staging_events FROM {}
    CREDENTIALS 'aws_access_key_id={};aws_secret_access_key={}'
    compupdate off region 'us-west-2'
    TIMEFORMAT as 'epochmillisecs'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    FORMAT AS JSON {};
""").format(LOG_DATA,KEY,SECRET,LOG_PATH)

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    CREDENTIALS 'aws_access_key_id={};aws_secret_access_key={}'
    compupdate off region 'us-west-2'
    FORMAT AS JSON 'auto'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""").format(SONG_DATA,KEY,SECRET,LOG_PATH)



songplay_table_insert = ("""
    INSERT INTO fact_songplay(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT to_timestamp(to_char(se.ts, '9999-99-99 99:99:99'),'YYYY-MM-DD HH24:MI:SS'),
            se.userId as user_id,
            se.level as level,
            ss.song_id as song_id,
            ss.artist_id as artist_id,
            se.sessionId as session_id,
            se.location as location,
            se.userAgent as user_agent
    FROM staging_events se
    JOIN staging_songs ss ON se.song = ss.title AND se.artist = ss.artist_name;
    """)

user_table_insert = ("""
    INSERT INTO dim_user(user_id, first_name, last_name, gender, level)
    SELECT DISTINCT userId as user_id,
            firstName as first_name,
            lastName as last_name,
            gender as gender,
            level as level
    FROM staging_events
    where userId IS NOT NULL;
    """)

song_table_insert = ("""
    INSERT INTO dim_song(song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id as song_id,
            title as title,
            artist_id as artist_id,
            year as year,
            duration as duration
    FROM staging_songs
    WHERE song_id IS NOT NULL;
    """)

artist_table_insert = ("""
    INSERT INTO dim_artist(artist_id, name, location, latitude, longitude)
    SELECT DISTINCT artist_id as artist_id,
            artist_name as name,
            artist_location as location,
            artist_latitude as latitude,
            artist_longitude as longitude
    FROM staging_songs
    where artist_id IS NOT NULL;
    """)

time_table_insert = ("""
    INSERT INTO dim_time(start_time, hour, day, week, month, year, weekday)
    SELECT distinct ts,
            EXTRACT(hour from ts),
            EXTRACT(day from ts),
            EXTRACT(week from ts),
            EXTRACT(month from ts),
            EXTRACT(year from ts),
            EXTRACT(weekday from ts)
    FROM staging_events
    WHERE ts IS NOT NULL;
    """)


create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries   = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries   = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
