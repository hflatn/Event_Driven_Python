import psycopg2
import transform
from transform import lambda_h
import json
import os
import boto3
from extract import pd
from sqlalchemy import create_engine


client = boto3.client('ssm')
endpoint = client.get_parameter(Name='Event_Driven_RDS_Endpoint')
password = client.get_parameter(Name='Event_Driven_RDS_Password')
port = client.get_parameter(Name='Event_Driven_RDS_Port')
username = client.get_parameter(Name='Event_Driven_RDS_Username')
database = client.get_parameter(Name='event_driven_rds_db')
    
host = endpoint['Parameter']['Value']
password = password['Parameter']['Value']
port = port['Parameter']['Value']
username = username['Parameter']['Value']
database = database['Parameter']['Value']
    
    
"""conn = psycopg2.connect(
        host = host,
        database = database,
        user = username,
        password = password
)"""

conn_string = f'postgresql://{username}:{password}@{host}/{database}'

        
def lambda_handler(event, context):
    
    db = create_engine(conn_string)
    conn_alch = db.connect()
    
    conn = psycopg2.connect(conn_string)
    cur = conn.cursor()
    
    # cur.execute("CREATE TABLE nyt_df_table (id serial PRIMARY KEY, Date DATE, Deaths INTEGER, Cases INTEGER);")
    # cur.execute("CREATE TABLE jh_df_us_table (id serial PRIMARY KEY, Date DATE, Deaths INTEGER, Cases INTEGER);")
    
    # sql11 = '''DROP TABLE nyt_df_table '''
    # cur.execute(sql11)
    
    
    #cur = conn.cursor()
    
    
    
    lambda_h(event, context)
    nyt_df = transform.nyt_df
    jh_df_us = transform.jh_df_us

        
    
    
    
    nyt_df.to_sql('nyt_df_table', con=conn_alch, if_exists='replace')
    # jh_df_us.to_sql('jh_df_us_table', con=conn, if_exists='replace')
    
    # conn = psycopg2.connect(conn_string)
    
    conn.autocommit = True
    
    
    
    sql1 = '''select * from nyt_df_table;'''
    cur.execute(sql1)
    for i in cur.fetchall():
        print(i)
    
    
    conn.commit()
    #cur.execute("SELECT * FROM nyt_df_table;")
    #print(cur.fetchall())
        
    
    conn.close()