import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, analytical_queries
import numpy as np
import matplotlib.pyplot as plt


"""
    This function loads data about log events related to user's activity on Sparkify music app and metadata related to songs stored in S3 in JSON format to the tables in Redshift cluster for staging purpose.
    
    INPUT
    cur cursor variable to database in Redshift cluster
    conn connection object to database in Redshift cluster
"""
def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        print ("About to execute query: ", query)
        cur.execute(query)
        conn.commit()
        print("Done executing query: ", query)

"""
    This function loads data about songs, users, artists, song plays etc. from staging tables into fact and dimension tables.
    
    INPUT
    cur cursor variable to database in Redshift cluster
    conn connection object to database in Redshift cluster
"""
def insert_tables(cur, conn):
    for query in insert_table_queries:
        print ("About to execute query: ", query)
        cur.execute(query)
        conn.commit()
        print("Done executing query: ", query)
        
"""
    This function analyzes data from the facts and dimension tables in Redshift cluster.
    
    INPUT
    cur cursor variable to database in Redshift cluster
    conn connection object to database in Redshift cluster
"""        
def analyse_data(cur, conn):
    for query in analytical_queries:
        print ("About to execute query: ", query)
        cur.execute(query)        
        data = np.array(cur.fetchall())
        print (data)
        print("Done executing query: ", query)
        
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    analyse_data(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()