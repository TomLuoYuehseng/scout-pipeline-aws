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
import io
import psutil


def initiate_client():
    actor = boto3.client('s3')
    return actor

def read_excel_from_s3(Input_Bucket, Input_Key, Sheet_Name):
  # Input_Bucket = 'cdl-scout'
  # Input_Key = 'Internal_Pricing_Exclusions_Tiers.xlsx'
  # Sheet_Name = 'Internal - Prices Exclusion'

  client = initiate_client()
  excel_obj = client.get_object(Bucket=Input_Bucket, Key=Input_Key)
  body = excel_obj['Body'].read()
  df = pd.read_excel(BytesIO(body), sheet_name=Sheet_Name)
  return df

def read_df_from_csv_on_s3(input_bucket, input_key):
    """ read a CSV from S3 and convert to dataframe"""
    client = initiate_client()
    response = client.get_object(Bucket=input_bucket, Key=input_key)
    df = pd.read_csv(response.get("Body"))
    return df

def write_df_to_csv_on_s3(dataframe, output_bucket, output_key):
    """ Write a dataframe to a CSV on S3 """
    print("Writing {} records to {}".format(len(dataframe), output_key))
    csv_buffer = io.StringIO()
    dataframe.to_csv(csv_buffer)
    s3_resource = boto3.resource('s3')
    s3_resource.Object(output_bucket, output_key).put(Body=csv_buffer.getvalue())

def initiate_rds_connection(rds_host, user_name, database_name, database_password, port):
    print(database_password)
    print(database_name)
    engine = create_engine('mysql+pymysql://' + user_name + ':' + database_password + '@' + rds_host + ':' + str(port) + '/' + database_name , echo=False)
    return engine

def get_secret():

    secret_name = "ScoutPipelineAwsStackshared-Kge2Cwhk6EA6"
    region_name = "us-east-2"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    # Decrypts secret using the associated KMS key.
    secret = get_secret_value_response['SecretString']
    return secret

def overwrite_scoutdb_table_noindex(result_table, db_table_name, engine):
    """
    This function creates a new RDS database table and writes records to it
    """
    result_table.to_sql(name=db_table_name, con=engine, if_exists = 'replace', index=False)
    
def overwrite_scoutdb_table(result_table, db_table_name,engine):
  result_table.to_sql(name=db_table_name, con=engine, if_exists = 'replace')

def scoutdb_tables(sql, engine):
    with engine.connect().execution_options(autocommit=True) as conn:
        query = conn.execute(text(sql))         
    df = pd.DataFrame(query.fetchall())
    return df

def grab_time_stamp():
  client = initiate_client()
  timestampfile = client.list_objects_v2(Bucket='cdl-scout',
                                         Delimiter="/",
                                         Prefix="Temp_Page_Source/")
  for i in timestampfile["Contents"]:
    if '.txt' in str(i):
      timestamp = i["Key"].replace("Temp_Page_Source/", '').replace('.txt', '') # noqa

  return timestamp

# Newly added for market basket mapping ETL
def create_competitor_key_id(df):
  key_val = df['competitor_key'].astype(str)
  df['competitor_key_id'] = [int(
    hashlib.sha256(
      val.encode('utf-8')).hexdigest(), 16) % 10**8 for val in key_val]
  return df