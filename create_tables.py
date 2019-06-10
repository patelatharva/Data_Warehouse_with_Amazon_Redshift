import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

"""
    This function drops all the tables from Redshift cluster database
    INPUT:
        cur cursor variable of database on Redshift cluster
        conn connection object to database on Redshift cluster
"""
def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

"""
    This function creates all the necessary tables in database on Redshift cluster.
    Tables created are:
        - staging_events
        - staging_songs
        - songplays
        - songs
        - artists
        - users
        - time
    INPUT:
        cur cursor variable of database on Redshift cluster
        conn connection object to database on Redshift cluster
"""
def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()