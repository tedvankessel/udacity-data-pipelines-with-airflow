"""
sqlqueries_tvk.py
used by: tvkDAGv2.py program
used for: Udacity Automate Data Piplines Project
2023-08-02
for details, sources etc. see README.md
"""
class SqlQueries:
    """ 
    this class defines SQL queries used by the tvkDAGv2.py program and related programs to create the tables used in the 
    Automate Data Pipelines Project
    """

    # create and insert data into the songplay fact table from staging tables
    songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS {} (
    songplay_id bigint IDENTITY(0,1) PRIMARY KEY,
    start_time varchar,
    user_id varchar,
    level varchar,
    song_id varchar,
    artist_id varchar,
    session_id varchar,
    location varchar,
    user_agent varchar
    )
    """)

    songplay_table_insert = ("""
    INSERT INTO {} (
        start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent
    ) 
    SELECT e.ts as start_time,
        e.userId as user_id,
        e.level as level,
        s.song_id as song_id,
        s.artist_id as artist_id,
        e.sessionId as session_id,
        e.location as location,
        e.userAgent as user_agent
    FROM staging_events e
    JOIN staging_songs s ON (e.song = s.title AND e.artist = s.artist_name) 
    """)

    # create and insert data into the user dimension table from staging events table

    user_table_create = ("""
    CREATE TABLE IF NOT EXISTS {} (
    user_id varchar PRIMARY KEY,
    first_name varchar,
    last_name varchar,
    gender varchar,
    level varchar
    )
    """)

    user_table_insert = ("""
    INSERT INTO {} (
        user_id,
        first_name,
        last_name,
        gender,
        level
    )
    SELECT DISTINCT e.userId as user_id,
    e.firstName as first_name,
    e.lastName as last_name,
    e.gender as gender,
    e.level as level
    FROM staging_events e
    WHERE e.userId IS NOT NULL;
    """)

    # create and insert data into the song dimension table from staging_songs table

    song_table_create = ("""
    CREATE TABLE IF NOT EXISTS song (
    song_id varchar PRIMARY KEY,
    title varchar,
    artist_id varchar,
    year varchar,
    duration float
    )
    """)

    song_table_insert = ("""
    INSERT INTO {} (
        song_id,
        title,
        artist_id,
        year,
        duration
    )
    SELECT DISTINCT s.song_id as song_id,
        s.title as title,
        s.artist_id as artist_id,
        s.year as year,
        s.duration as duration
    FROM staging_songs s
    """)

    # create and insert data into the artist dimension table from staging_songs table

    artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS {} (
    artist_id varchar PRIMARY KEY,
    name varchar,
    location varchar,
    latitude numeric,
    longitude numeric
    )
    """)

    artist_table_insert = ("""
    INSERT INTO {} (
        artist_id,
        name,
        location,
        latitude,
        longitude
    )
    SELECT DISTINCT s.artist_id as artist_id,
        s.artist_name as name,
        s.artist_location as location,
        s.artist_latitude as latitude,
        s.artist_longitude as longitude
    FROM staging_songs s
    """)

    # create and insert data into the time dimension table from staging_events table

    time_table_create = ("""
    CREATE TABLE IF NOT EXISTS {} (
    start_time timestamp PRIMARY KEY,
    hour varchar,
    day varchar,
    week varchar,
    month varchar,
    year varchar,
    weekday varchar
    )
    """)

    time_table_insert = ("""
    INSERT INTO {} (
        start_time,
        hour,
        day,
        week,
        month,
        year,
        weekday
    )
    SELECT DISTINCT timestamp 'epoch' + CAST(e.ts AS BIGINT)/1000 * interval '1 second' as start_time,
        extract(HOUR FROM timestamp 'epoch' + CAST(e.ts AS BIGINT)/1000 * interval '1 second') as hour,
        extract(DAY FROM timestamp 'epoch' + CAST(e.ts AS BIGINT)/1000 * interval '1 second') as day,
        extract(WEEK FROM timestamp 'epoch' + CAST(e.ts AS BIGINT)/1000 * interval '1 second') as week,
        extract(MONTH FROM timestamp 'epoch' + CAST(e.ts AS BIGINT)/1000 * interval '1 second') as month,
        extract(YEAR FROM timestamp 'epoch' + CAST(e.ts AS BIGINT)/1000 * interval '1 second') as year,
        extract(DAY FROM timestamp 'epoch' + CAST(e.ts AS BIGINT)/1000 * interval '1 second') as weekday
    FROM staging_events e
    """)

    # create the staging_events staging table

    staging_events_table_create = ("""
    CREATE TABLE IF NOT EXISTS {} (
    artist varchar,
    auth varchar,
    "firstName" varchar,
    gender varchar,
    "itemInSession" int,
    "lastName" varchar,
    length float,
    level varchar,
    location varchar,
    method varchar,
    page varchar,
    registration float,
    "sessionId" int,
    song varchar,
    status int,
    ts varchar,
    "userAgent" varchar,
    "userId" varchar
    )
    """)

    # create the staging_songs staging table

    staging_songs_table_create  = ("""
    CREATE TABLE IF NOT EXISTS {} (
    num_songs int,
    artist_id varchar,
    artist_latitude numeric,
    artist_longitude numeric,
    artist_location varchar,
    artist_name varchar,
    song_id varchar,
    title varchar,
    duration float,
    year int
    )
    """)

    # The dictionary below defines the tests to be used in the data_quality evaluation
    # and the expected result

    dq_checks=[
        {'check_sql': "SELECT COUNT(*) FROM users WHERE user_id is null", 'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM song WHERE song_id is null", 'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM artist WHERE artist_id is null", 'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM time WHERE start_time is null", 'expected_result': 0}
    ]
