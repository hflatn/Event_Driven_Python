import transform
from transform import lambda_h
from notification import notify
import psycopg2
import boto3
import json
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
    
conn_string = f'postgresql://{username}:{password}@{host}/{database}'

db = create_engine(conn_string)
conn_alch = db.connect()
conn = psycopg2.connect(conn_string)
cur = conn.cursor()

def lambda_handler(event, context):
    
    
    # Initially create tables and/or drop for testing purposes
    # drop_table = '''DROP TABLE jh_df_us_table '''
    # cur.execute(drop_table)
    # cur.execute("CREATE TABLE nyt_df_table (id serial PRIMARY KEY, Date DATE, Deaths INTEGER, Cases INTEGER);")
    # cur.execute("CREATE TABLE jh_df_us_table (id serial PRIMARY KEY, Date DATE, Deaths INTEGER, Cases INTEGER);")
    # jh_df.to_sql('jh_df_us_table', con=conn_alch, if_exists='replace', index=False)
    
    # Reads current/old data
    nyt_df_db = pd.read_sql_query('select * from "nyt_df_table"',con=conn_alch)
    jh_df_db = pd.read_sql_query('select * from "jh_df_us_table"',con=conn_alch)
    # print(nyt_df_db)
    # print(jh_df_db)
    
    
    # Inserts/replaces dataframe with two rows removed for testing purposes
    # nyt_df_db = nyt_df_db.drop(nyt_df_db.tail(2).index)
    jh_df_db = jh_df_db.drop(jh_df_db.tail(2).index)
    # print(nyt_df_db.tail())
    # nyt_df_db.to_sql('nyt_df_table', con=conn_alch, if_exists='replace', index=False)
    jh_df_db.to_sql('jh_df_us_table', con=conn_alch, if_exists='replace', index=False)
    # print(jh_df_db.tail())
    # print(jh_df_db.info())
    
    
    # Grabs fresh data
    lambda_h(event, context)
    nyt_df = transform.nyt_df
    jh_df = transform.jh_df_us

    
    # Compares old with new data
    nyt_merged = nyt_df_db.merge(nyt_df, indicator = True, how = 'outer')
    jh_merged = jh_df_db.merge(jh_df, indicator = True, how = 'outer')
    # print(jh_merged)
    # print(jh_merged.info())
    
    # creates df with only the new rows then appends to dataframe in DB
    nyt_new_data = nyt_merged.loc[lambda x: x['_merge'] != 'both']
    nyt_new_data = nyt_new_data.drop(['_merge'], axis=1)
    jh_new_data = jh_merged.loc[lambda x: x['_merge'] != 'both']
    jh_new_data = jh_new_data.drop(['_merge'], axis=1)
    print(jh_new_data)
    print(jh_new_data.info())
    
    # Appends new data to db
    nyt_new_data.to_sql('nyt_df_table', con=conn_alch, if_exists='append', index=False)
    jh_new_data.to_sql('jh_df_us_table', con=conn_alch, if_exists='append', index=False)
       
    # Checkign to make sure the db has been update
    nyt_df_db = pd.read_sql_query('select * from "nyt_df_table"',con=conn_alch)
    jh_df_db = pd.read_sql_query('select * from "jh_df_us_table"',con=conn_alch)
    # print(jh_df_db.tail())
    
    # Publishes updated row to subscribers of topic
    notify(nyt_new_data, jh_new_data)
        
    conn.close()
    conn_alch.close()