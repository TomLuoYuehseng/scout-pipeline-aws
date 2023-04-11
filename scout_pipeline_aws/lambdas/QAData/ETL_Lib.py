# from asyncio.windows_events import NULL
import pandas as pd
from io import StringIO, BytesIO
import boto3
import hashlib
import os
import sys
import logging
import pymysql
import json
from sqlalchemy import create_engine
from sqlalchemy.sql import text
#import awswrangler as wr
from botocore.exceptions import ClientError


def initiate_client():
    actor = boto3.client('s3')
    return actor


def read_df_from_csv_on_s3(input_bucket, input_key):
    """ read a CSV from S3 and convert to dataframe"""
    client = initiate_client()
    response = client.get_object(Bucket=input_bucket, Key=input_key)
    df = pd.read_csv(response.get("Body"))
    return df


def initiate_rds_connection(rds_host, user_name, database_name, database_password, port):
    print(database_password)
    print(database_name)
    engine = create_engine('mysql+pymysql://' + user_name + ':' + database_password + '@' + rds_host + ':' + str(port) + '/' + database_name , echo=False)
    return engine

def overwrite_scoutdb_table_noindex(qa_table, table_name, engine):
    """
    This function creates a new RDS database table and writes records to it
    """
    qa_table.to_sql(name=table_name, con=engine, if_exists = 'replace', index=False)
    
    '''
    conn = initiate_rds_connection(rds_host, database_name, database_password)

    wr.mysql.to_sql(
        df=qa_table,
        table="PBI_Scout_Raw_Data",
        schema="test",
        con=conn
    )
    conn.close()
    '''
def query_all_data(table_name, engine):
    sql = f'''SELECT * FROM {table_name};'''
    with engine.connect().execution_options(autocommit=True) as conn:
        query = conn.execute(text(sql))         
    df = pd.DataFrame(query.fetchall())
    return df