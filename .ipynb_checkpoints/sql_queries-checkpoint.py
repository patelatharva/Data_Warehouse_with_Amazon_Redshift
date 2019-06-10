import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE staging_events (
        event_id integer IDENTITY(0, 1) PRIMARY KEY,
        artist varchar (max) DEFAULT '',
        auth varchar (max) DEFAULT '',
        firstName varchar (max) DEFAULT '',
        gender varchar (max) DEFAULT '',
        itemInSession integer,
        lastName varchar (max) DEFAULT '',
        length numeric,
        level varchar (max) DEFAULT '',
        location varchar (max) DEFAULT '',
        method varchar (max) DEFAULT '',
        page varchar (max) DEFAULT '',
        registration bigint,
        sessionId integer,
        song varchar (max) DEFAULT '',
        status integer,
        ts bigint, 
        userAgent varchar (max) DEFAULT '',
        userId varchar (max) DEFAULT ''
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs (
        num_songs integer,
        artist_id varchar (max) NOT NULL,
        artist_latitude numeric,
        artist_longitude numeric,
        artist_location varchar (max),
        artist_name varchar (max) NOT NULL,
        song_id varchar (max) NOT NULL,
        title varchar (max) NOT NULL,
        duration numeric NOT NULL,
        year integer NOT NULL,
        PRIMARY KEY (song_id)
    );
""")

songplay_table_create = ("""
    CREATE TABLE songplays (
        songplay_id integer IDENTITY(0,1) PRIMARY KEY NOT NULL,
        start_time datetime NOT NULL,
        user_id varchar (max) NOT NULL,
        level varchar (max) DEFAULT '',
        song_id varchar (max) NOT NULL,
        artist_id varchar (max) NOT NULL,
        session_id integer NOT NULL,
        location varchar (max) DEFAULT '',
        user_agent varchar (max) DEFAULT '',
        foreign key (start_time) references time (start_time),
        foreign key (user_id) references users (user_id),
        foreign key (song_id) references songs (song_id),        
        foreign key (artist_id) references artists (artist_id)        
    );
""")

user_table_create = ("""
    CREATE TABLE users (
        user_id varchar (max) PRIMARY KEY NOT NULL,
        first_name varchar (max) DEFAULT '',
        last_name varchar (max) DEFAULT '',
        gender varchar (max) DEFAULT '',
        level varchar (max) DEFAULT ''
    );
""")

song_table_create = ("""
    CREATE TABLE songs (
        song_id varchar (max) PRIMARY KEY NOT NULL, 
        title varchar (max) NOT NULL,
        artist_id varchar (max) NOT NULL,
        year integer NOT NULL,
        duration numeric NOT NULL
    );
""")

artist_table_create = ("""
    CREATE TABLE artists (
        artist_id varchar (max) PRIMARY KEY NOT NULL,
        name varchar (max) NOT NULL,
        location varchar (max) DEFAULT '',
        lattitude numeric,
        longitude numeric        
    );
""")

time_table_create = ("""
    CREATE TABLE time (
        start_time datetime PRIMARY KEY NOT NULL,
        hour integer NOT NULL,
        day integer  NOT NULL,
        week integer  NOT NULL,
        month integer  NOT NULL,
        year integer NOT NULL,
        weekday integer NOT NULL
    );
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events from {}
    iam_role {}
    json {}
    region 'us-west-2';
""").format(config['S3']['LOG_DATA'], config["IAM_ROLE"]["ARN"], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
    COPY staging_songs from {}    
    iam_role {}
    format as json 'auto'
    region 'us-west-2';    
""").format(config['S3']['SONG_DATA'], config["IAM_ROLE"]["ARN"])

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
        user_agent
        )        
    SELECT
        timestamp 'epoch' + e.ts/1000 * interval '1 second' AS start_time,
        e.userId as user_id,
        e.level,
        s.song_id,
        s.artist_id,
        e.sessionId as session_id,
        e.location,
        e.userAgent as user_agent       
    FROM staging_events e
    JOIN staging_songs s ON (e.song=s.title AND e.length=s.duration AND e.artist=s.artist_name)
    WHERE e.page='NextSong';
""")

user_table_insert = ("""
    INSERT INTO users (user_id,
        first_name,
        last_name,
        gender,
        level)
    SELECT
        DISTINCT e.userId as user_id,
        e.firstName as first_name,
        e.lastName as last_name,
        e.gender,
        e.level
    FROM
        staging_events e;    
    
""")

song_table_insert = ("""
    INSERT INTO songs (
        song_id, 
        title,
        artist_id,
        year,
        duration
    )
    SELECT
        s.song_id,
        s.title,
        s.artist_id,
        s.year,
        s.duration
    FROM
        staging_songs s;
""")

artist_table_insert = ("""
    INSERT INTO artists (
        artist_id,
        name,
        location,
        lattitude,
        longitude
    )
    SELECT
        s.artist_id,
        s.artist_name as name,
        s.artist_location as location,
        s.artist_latitude as latitude,
        s.artist_longitude as longitude
    FROM 
        staging_songs s;
""")

time_table_insert = ("""
    INSERT INTO time (
        start_time,
        hour,
        day,
        week,
        month,
        year,
        weekday
    )
    SELECT
        e.start_time,
        extract(hour from e.start_time),
        extract(day from e.start_time),
        extract(week from e.start_time),
        extract(month from e.start_time),
        extract(year from e.start_time),
        extract(weekday from e.start_time)
    FROM        
        (SELECT timestamp 'epoch' + e.ts/1000 * interval '1 second' as start_time
        FROM staging_events e) as e;
""")

top_10_songs = ("""
    SELECT s.title as Song, a.name as Artist, count(sp.start_time) as Num_of_song_plays
    FROM
        songplays sp
        JOIN songs s ON (s.song_id=sp.song_id)
        JOIN artists a ON (a.artist_id=sp.artist_id)
    WHERE sp.start_time <= TO_DATE('30-11-2018', 'DD-MM-YYYY') AND sp.start_time > TO_DATE('15-11-2018', 'DD-MM-YYYY')
    GROUP BY Song, Artist
    ORDER BY Num_of_song_plays desc
    LIMIT 10;        
""")

average_number_of_songs_played_per_during_specified_period = ("""
    SELECT number_of_song_plays_by_user_by_day.day as day, avg(number_of_song_plays_by_user_by_day.number) as average
    FROM (
        SELECT TO_CHAR(sp.start_time, 'DD-Month-YY') as day, sp.user_id, count(sp.start_time) as number
        FROM (
            SELECT songplay_id, start_time, user_id
            FROM songplays sp
            GROUP BY songplay_id, start_time, user_id
            HAVING sp.start_time <= TO_DATE('30-11-2018', 'DD-MM-YYYY') AND sp.start_time > TO_DATE('15-11-2018', 'DD-MM-YYYY')
        ) as sp         
        GROUP BY day, sp.user_id
    ) as number_of_song_plays_by_user_by_day
    GROUP BY day;
""")

number_of_song_played_per_day_during_specified_period = ("""
        SELECT TO_CHAR(sp.start_time, 'DD-Month-YY') as day, count(sp.start_time) as number_of_song_plays
        FROM (
            SELECT songplay_id, start_time, user_id
            FROM songplays sp
            GROUP BY songplay_id, start_time, user_id
            HAVING sp.start_time <= TO_DATE('30-11-2018', 'DD-MM-YYYY') AND sp.start_time > TO_DATE('15-11-2018', 'DD-MM-YYYY')
        ) as sp         
        GROUP BY day;
""")

number_of_users_who_listened_to_songs_during_specified_period = ("""
    SELECT TO_CHAR(sp.start_time, 'DD-Month-YY') as day, count(distinct user_id)
    FROM songplays sp
    WHERE sp.start_time <= TO_DATE('30-11-2018', 'DD-MM-YYYY') AND sp.start_time > TO_DATE('15-11-2018', 'DD-MM-YYYY')
    GROUP BY day;
""")

create_table_queries = [
    staging_events_table_create, 
    staging_songs_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create,
    songplay_table_create,
]
drop_table_queries = [
    staging_events_table_drop, 
    staging_songs_table_drop, 
    songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [
    staging_events_copy,
    staging_songs_copy
]
insert_table_queries = [
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert
]
analytical_queries = [
    top_10_songs, 
    average_number_of_songs_played_per_during_specified_period,
    number_of_song_played_per_day_during_specified_period,
    number_of_users_who_listened_to_songs_during_specified_period
]