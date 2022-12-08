import psycopg2
import transform
import config

nyt_df = transform.nyt_df
jh_df_us = transform.jh_df_us

conn = psycopg2.connect("dbname =  user=postgres")