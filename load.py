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
    
    # Declares global so the notify function can access them
    global nyt_new_data
    global jh_new_data
    
    
    # Grabs Fresh Data
    lambda_h(event, context)
    nyt_df = transform.nyt_df
    jh_df = transform.jh_df_us
    
    # Drops table to test table creation in if else statement
    # cur.execute("""DROP TABLE nyt_df_table""")
    # cur.execute("""DROP TABLE jh_df_table""")
    # conn.commit()
    
    
    # Checks whether the table exists or not and creates new table or appends to existing table based on boolean value
    cur.execute("SELECT EXISTS (SELECT 1 from information_schema.tables where table_name=%s)", ('nyt_df_table',))
    tableExists = cur.fetchone()[0]
    if bool(tableExists):
        
        # Reads current/old data 
        nyt_df_db = pd.read_sql_query('select * from "nyt_df_table"',con=conn_alch)
        
        # Compares old with new data
        nyt_merged = nyt_df_db.merge(nyt_df, indicator = True, how = 'outer')
        
        # Compares old with new data
        nyt_merged = nyt_df_db.merge(nyt_df, indicator = True, how = 'outer')
        
        # Creates df with only the new data
        nyt_new_data = nyt_merged.loc[lambda x: x['_merge'] != 'both']
        nyt_new_data = nyt_new_data.drop(['_merge'], axis=1)
        
        # Appends new data to db
        nyt_new_data.to_sql('nyt_df_table', con=conn_alch, if_exists='append', index=False)
        
        conn.commit()
    
    else:
        # Creates new table with all data retrieved
        nyt_df.to_sql('nyt_df_table', con=conn_alch, if_exists='replace', index=False)
        nyt_new_data = nyt_df
        
        conn.commit()
        
    
    # Checks whether the table exists or not and creates new table or appends to existing table based on boolean value
    cur.execute("SELECT EXISTS (SELECT 1 from information_schema.tables where table_name=%s)", ('jh_df_table',))
    tableExists = cur.fetchone()[0]
    if bool(tableExists):
        
        # Reads current/old data
        jh_df_db = pd.read_sql_query('select * from "jh_df_table"',con=conn_alch)
        
        # Simulates the db missing the two most recent rows of data
        # jh_df_db.drop(jh_df_db.tail(2).index, inplace = True)
        # jh_df_db.to_sql('jh_df_table', con=conn_alch, if_exists='replace', index=False)
        
        # Compares old with new data
        jh_merged = jh_df_db.merge(jh_df, indicator = True, how = 'outer')
        
        # Creates df with only the new data
        jh_new_data = jh_merged.loc[lambda x: x['_merge'] != 'both']
        jh_new_data = jh_new_data.drop(['_merge'], axis=1)
        
        # Appends new data to db
        jh_new_data.to_sql('jh_df_table', con=conn_alch, if_exists='append', index=False)
        conn.commit()
    
    else:
        # Creates new table with all data retrieved
        jh_df.to_sql('jh_df_table', con=conn_alch, if_exists='replace', index=False)
        jh_new_data = jh_df
        
        conn.commit()


    # Checking to make sure the db has been update
    # nyt_df_db = pd.read_sql_query('select * from "nyt_df_table"',con=conn_alch)
    # jh_df_db = pd.read_sql_query('select * from "jh_df_table"',con=conn_alch)
    
    
    # Publishes updated row to subscribers of topic
    notify(nyt_new_data, jh_new_data)
        
    conn.close()
    conn_alch.close()