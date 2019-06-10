# Building Data Warehouse with AWS Redshift

In this project I created an ETL pipeline for a database hosted on Redshift. I loaded data from S3 to staging tables on Redshift and execute SQL statements that created the analytics tables from these staging tables.

## Analytics Team's Requirement
A fictitious music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The task is to build an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a fact table and set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to.

I completed the task by creating ETL pipeline and tested it by running some sample analytical queries against the database.

## Solution
### Database Schema
I decided to build database using following Star schema. In this schema, the *songplays* table serves as fact table containing information related to user's activity of listening to songs at specific times. Tables called *users, artists, time and songs* serve as dimension tables, which are referenced through foreign keys present in *songplays* fact table.

![](song_plays_database_ERD.png)

### ETL Pipeline
#### Data
* The data about events generated as part of user activity for e.g. listening next song, are stored as separate log files in JSON format in Amazon S3. This log files also contains information about user like first name, last name, location etc. They also contain information about the event like timestamp when user starts listening song, title of the song and its id, song's time length, artist name etc.
* The metadata about a large collection of songs is stored as files in JSON format in a separate directory on Amazon S3.

#### Loading into Redshift
I created two staging tables called *staging_events* and *staging_songs* to first load the data from JSON files in S3 into Redshift before doing further processing. I executed the COPY commands available in S3 that allows directly loading data of user activity log and songs in JSON format from S3 to database tables in Redshift Cluster. It's worth noting that thanks to this feature of directly importing data from S3 to Redshift Cluster, I didn't have to copy any data in local storage of the system where my ETL python script was executed.

    # This is how the queries with COPY commands for loading data from S3 to Redshift Cluster looks like  
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

Then I ran different SQL INSERT queries by selecting data from staging tables to load it into facts table *songplays* and dimension tables like *users, artists and time*. Storing the data related to events like playing songs in separate table which contains foreign keys to dimension tables allows to run analytical queries while minimizing as data redundancy.

The dimension tables are designed to contain majority of the information related to the entity being represented by them and there by minimizing JOIN operations required to access information about any dimension while executing analytical queries.

#### Organization of solution code in files
The code has been modularized into different files.
* `create_tables.py` is a python script useful for dropping existing tables and creating new ones of types staging, facts and dimensions in AWS Redshift cluster database.
* `etl.py` is a python script for performing ETL operation i.e. loading events and songs data from S3 into Redshift cluster and filling up staging, facts and dimensions tables. It also executes some sample analytical queries and prints the results.
* `sql_queries.py` contains all the declarations of the queries that are used by *create_tables.py* for creating and dropping tables and *etl.py* for copying data from S3 to staging using COPY commands and from staging tables to facts and dimensions tables using SQL INSERT queries. It also declares some analytical queries used by *etl.py* to test them against loaded Redshift database.
* `dwh.cfg` specifies configuration values for connecting to Redshift cluster database and locations of data files in S3.

#### How to run the solution
The python scripts in this project are meant to be run by Python version 3+. Following commands are to be run after setting current directory of terminal to be project's directory.
To install the dependencies for the file, one can run:

    pip install -r requirements.txt

The scripts assumes the Redshift cluster to be available and is accepting TCP connection on port 5439 from the machine on which the scripts will be executed. The configuration details related to Redshift cluster is stored in `dwh.cfg` file in project directory. One must specify details like Redshift cluster hostname, database name, username, password and database port. It also expects IAM Role that is to be assumed the database cluster to COPY data from S3. Finally the locations of files where the data about user activity logs and songs are stored and the path to file containing JSONpaths for converting log files from JSON to database entries has to be specified in `dwh.cfg`.

To create tables in Redshift, run following command in the terminal with current directory set to project's directory.

    python create_tables.py

To start ETL process of loading data from S3 to staging area, from staging area to facts and dimension tables and finally running some sample queries to test everything, run following command.

    python etl.py

### Testing against sample analytical queries
I tested the database of facts and dimension tables against following sample queries that would show some metrics that senior management of company might interested in keeping track of through dashboards and the results were as expected.
#### 1: Get top 10 songs which are listened the highest number of times by users in specified time period. 
This type of query would help getting idea about the most trending songs on the app in the last week or any specific time period.

    SELECT s.title as song, a.name as artist, count(sp.start_time) as num_of_song_plays
    FROM
        songplays sp
        JOIN songs s ON (s.song_id=sp.song_id)
        JOIN artists a ON (a.artist_id=sp.artist_id)
    WHERE sp.start_time <= TO_DATE('30-11-2018', 'DD-MM-YYYY') AND sp.start_time > TO_DATE('15-11-2018', 'DD-MM-YYYY')
    GROUP BY Song, Artist
    ORDER BY Num_of_song_plays desc
    LIMIT 10;        
The output of the query formatted by numpy array was:

    Song Title | Artist Name | Number of song plays 
    [["You're The One" 'Dwight Yoakam' '19']
     ['Secrets' 'Carleen Anderson' '18']
     ['Home' 'Frozen Plasma' '16']
     ['Home' 'Working For A Nuclear Free City' '16']
     ['Broken' 'Ours' '14']
     ['Stronger' 'Taxiride' '12']
     ['Overture' 'Blood_ Sweat & Tears' '10']
     ['From The Ritz To The Rubble' 'Arctic Monkeys' '9']
     ['Angie (1993 Digital Remaster)' 'The Rolling Stones' '8']
     ['Home' 'Eli Young Band' '8']]


#### 2: Get total number of songs listened by users per day in the specified time period.
This type of query would help the management getting idea about the total interaction and usage of app by all users in the recent time period.

    SELECT TO_CHAR(sp.start_time, 'DD-Month-YY') as day, count(sp.start_time) as number_of_song_plays
        FROM (
            SELECT songplay_id, start_time, user_id
            FROM songplays sp
            GROUP BY songplay_id, start_time, user_id
            HAVING sp.start_time <= TO_DATE('30-11-2018', 'DD-MM-YYYY') AND sp.start_time > TO_DATE('15-11-2018', 'DD-MM-YYYY')
        ) as sp         
        GROUP BY day;
The output of the query formatted by numpy array was:

    Date | Number of song plays
    [['16-November -18' '54']
     ['17-November -18' '17']
     ['20-November -18' '44']
     ['18-November -18' '16']
     ['24-November -18' '43']
     ['23-November -18' '36']
     ['29-November -18' '47']
     ['28-November -18' '65']
     ['25-November -18' '3']
     ['15-November -18' '76']
     ['19-November -18' '52']
     ['22-November -18' '15']
     ['21-November -18' '65']
     ['26-November -18' '48']
     ['27-November -18' '61']]

#### 3: Get average number of songs listened by each user per day in the specified time period.
This type of query would help getting idea on what is the level of engagement of each user on average with the app in recent time period.

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
The output of the query formatted by numpy array was:

    Date | Average number of song plays per user
    [['16-November -18' '4']
     ['20-November -18' '4']
     ['24-November -18' '4']
     ['23-November -18' '2']
     ['29-November -18' '4']
     ['28-November -18' '5']
     ['25-November -18' '1']
     ['17-November -18' '3']
     ['18-November -18' '16']
     ['15-November -18' '6']
     ['19-November -18' '6']
     ['21-November -18' '7']
     ['22-November -18' '2']
     ['26-November -18' '3']
     ['27-November -18' '6']]

#### 4: Get total number of distinct users that listened to any song on each day of specified time period.
This type of query would help getting idea about number of active users of the app in recent time period.

    SELECT TO_CHAR(sp.start_time, 'DD-Month-YY') as day, count(distinct user_id)
    FROM songplays sp
    WHERE sp.start_time <= TO_DATE('30-11-2018', 'DD-MM-YYYY') AND sp.start_time > TO_DATE('15-11-2018', 'DD-MM-YYYY')
    GROUP BY day;
The output of the query formatted by numpy array was:

    Date | Number of users who listened to any song
    [['16-November -18' '12']
     ['20-November -18' '11']
     ['24-November -18' '9']
     ['23-November -18' '13']
     ['25-November -18' '2']
     ['29-November -18' '11']
     ['28-November -18' '11']
     ['17-November -18' '5']
     ['18-November -18' '1']
     ['15-November -18' '11']
     ['19-November -18' '8']
     ['22-November -18' '7']
     ['21-November -18' '9']
     ['26-November -18' '14']
     ['27-November -18' '9']]

