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


def initiate_client():
    actor = boto3.client('s3')
    return actor


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


def back_out_logic(table):
  # table = for_bo.copy()
  table = table.fillna('NaN')
  # table.to_csv('/DataDrive/Data Science/Marcia/Repo/compassdigital.ScOUT/Scout_Pipeline/ETL/bo_test.csv', index = False)
  price_ref_table = table[["standardized_competitor_name", "competitor_name", "competitor_key", "market_basket_name", "confidence", "menu_item_price_clean"]].drop_duplicates()
  # price_ref_table.to_csv("/DataDrive/Data Science/Marcia/price_ref_table.csv")
  
  # Normal Items
  normal_items = table[(table.Operation=='NaN')  & (table.Item_to_Back_Out_1=='NaN') & (table.Item_to_Back_Out_2=='NaN') & (table.Item_to_Add_1=='NaN')]
  normal_items['Ala_Carte_Price'] = normal_items['menu_item_price_clean']
  
  # Divide Only
  divide_items = table[(table.Operation!='NaN')  & (table.Item_to_Back_Out_1=='NaN') & (table.Item_to_Back_Out_2=='NaN') & (table.Item_to_Add_1=='NaN')]
  divide_items['menu_item_price_clean'] = divide_items['menu_item_price_clean'].apply(lambda x: float(x))
  divide_items['Ala_Carte_Price'] = divide_items['menu_item_price_clean'] / divide_items['Digit']
  divide_items['Ala_Carte_Price'] = divide_items['Ala_Carte_Price'].apply(lambda x: float(x))
  divide_items['Ala_Carte_Price'] = round((divide_items['Ala_Carte_Price']).apply(pd.to_numeric), 2)
  
  # Divide w/ 1 back out
  divide_onebo_items = table[(table.Operation!='NaN')  & (table.Item_to_Back_Out_1!='NaN') & (table.Item_to_Back_Out_2=='NaN') & (table.Item_to_Add_1=='NaN')]
  if len(divide_onebo_items) != 0:
    # find back out items and their prices
    bo_1 = divide_onebo_items[["standardized_competitor_name", "competitor_name", "competitor_key", "Item_to_Back_Out_1", "Back_Out_1_Tier"]].drop_duplicates()
    bo_1 = bo_1.rename({'Item_to_Back_Out_1': 'market_basket_name', 'Back_Out_1_Tier': 'confidence'}, axis = 1)
    bo_1 = pd.merge(bo_1, price_ref_table, on=['standardized_competitor_name', 'competitor_name', 'competitor_key', 'market_basket_name', 'confidence'])
    bo_1 = bo_1.rename({'market_basket_name': 'Item_to_Back_Out_1', 'confidence': 'Back_Out_1_Tier', 'menu_item_price_clean':'backout_1_price'}, axis = 1)
    # combine
    divide_onebo_final = pd.merge(divide_onebo_items, bo_1, on = ['standardized_competitor_name', 'competitor_name', 'competitor_key', 'Item_to_Back_Out_1', 'Back_Out_1_Tier'])
    divide_onebo_final['Ala_Carte_Price'] = round(((divide_onebo_final['menu_item_price_clean'] - divide_onebo_final['backout_1_price']) / divide_onebo_final['Digit']).apply(pd.to_numeric), 2)
    divide_onebo_final = divide_onebo_final.drop(columns = ['backout_1_price'])
  else:
    divide_onebo_final = pd.DataFrame()
  
  # Divide w/ 2 back out
  divide_twobo_items = table[(table.Operation!='NaN')  & (table.Item_to_Back_Out_1!='NaN') & (table.Item_to_Back_Out_2!='NaN') & (table.Item_to_Add_1=='NaN')]
  if len(divide_twobo_items) != 0:
    # find back out items and their prices - first backout
    bo_21 = divide_twobo_items[["standardized_competitor_name", "competitor_name", "competitor_key", "Item_to_Back_Out_1", "Back_Out_1_Tier"]]
    bo_21 = bo_21.rename({'Item_to_Back_Out_1': 'market_basket_name', 'Back_Out_1_Tier': 'confidence'}, axis = 1)
    bo_21 = pd.merge(bo_21, price_ref_table, on=['standardized_competitor_name', 'competitor_name', 'competitor_key', 'market_basket_name', 'confidence'])
    bo_21 = bo_21.rename({'market_basket_name': 'Item_to_Back_Out_1', 'confidence': 'Back_Out_1_Tier', 'menu_item_price_clean':'backout_1_price'}, axis = 1)
    # find back out items and their prices - second backout
    bo_22 = divide_twobo_items[["standardized_competitor_name", "competitor_name", "competitor_key", "Item_to_Back_Out_2", "Back_Out_2_Tier"]]
    bo_22 = bo_22.rename({'Item_to_Back_Out_2': 'market_basket_name', 'Back_Out_2_Tier': 'confidence'}, axis=1)
    bo_22 = pd.merge(bo_22, price_ref_table, on = ['standardized_competitor_name', 'competitor_name', 'competitor_key', 'market_basket_name', 'confidence'])
    bo_22 = bo_22.rename({'market_basket_name': 'Item_to_Back_Out_2', 'confidence': 'Back_Out_2_Tier', 'menu_item_price_clean':'backout_2_price'}, axis = 1)
    #combine
    bo_2full = pd.merge(divide_twobo_items, bo_21, on = ['standardized_competitor_name', 'competitor_name', 'competitor_key', 'Item_to_Back_Out_1', 'Back_Out_1_Tier'])
    divide_twobo_final = pd.merge(bo_2full, bo_22, on = ['standardized_competitor_name', 'competitor_name', 'competitor_key', 'Item_to_Back_Out_2', 'Back_Out_2_Tier'])
    divide_onebo_final['Ala_Carte_Price'] = round(((divide_twobo_final['menu_item_price_clean'] - divide_twobo_final['backout_1_price'] - divide_twobo_final['backout_2_price']) / divide_twobo_final['Digit']).apply(pd.to_numeric), 2)
    divide_twobo_final = divide_twobo_final.drop(columns = ["backout_1_price", "backout_2_price"])
  else:
    divide_twobo_final = pd.DataFrame()
  
  # 1 back out
  one_bo_item = table[(table.Operation=='NaN')  & (table.Item_to_Back_Out_1!='NaN') & (table.Item_to_Back_Out_2=='NaN') & (table.Item_to_Add_1=='NaN')] 
  
  if len(one_bo_item) != 0:
    # find back out items and their prices
    bo_3 = one_bo_item[["standardized_competitor_name", "competitor_name", "competitor_key", "Item_to_Back_Out_1", "Back_Out_1_Tier"]].drop_duplicates()
    bo_3 = bo_3.rename({'Item_to_Back_Out_1':'market_basket_name', 'Back_Out_1_Tier':'confidence'}, axis = 1)
    bo_3 = pd.merge(bo_3, price_ref_table, on=['standardized_competitor_name', 'competitor_name', 'competitor_key', 'market_basket_name', 'confidence'])
    bo_3 = bo_3.rename({'market_basket_name': 'Item_to_Back_Out_1', 'confidence': 'Back_Out_1_Tier', 'menu_item_price_clean':'backout_1_price'}, axis = 1)
    # onebo_final.to_csv('/DataDrive/Data Science/Marcia/Repo/compassdigital.ScOUT/Scout_Pipeline/ETL/bo_test.csv')

    # combine
    onebo_final = pd.merge(one_bo_item, bo_3, on = ['standardized_competitor_name', 'competitor_name', 'competitor_key', 'Item_to_Back_Out_1', 'Back_Out_1_Tier'])
    onebo_final['Ala_Carte_Price'] = round((onebo_final['menu_item_price_clean'] - onebo_final['backout_1_price']).apply(pd.to_numeric), 2)
    # onebo_final.to_csv('/DataDrive/Data Science/Marcia/Repo/compassdigital.ScOUT/Scout_Pipeline/ETL/bo_test2.csv')

    onebo_final = onebo_final.drop(columns = ['backout_1_price'])
  else:
    onebo_final = pd.DataFrame()
  
  # 2 back out
  twobo_items = table[(table.Operation=='NaN')  & (table.Item_to_Back_Out_1!='NaN') & (table.Item_to_Back_Out_2!='NaN') & (table.Item_to_Add_1=='NaN')]
  if len(twobo_items) != 0:
    # find back out items and their prices - first backout
    bo_41 = twobo_items[["standardized_competitor_name", "competitor_name", "competitor_key", "Item_to_Back_Out_1", "Back_Out_1_Tier"]]
    bo_41 = bo_41.rename({'Item_to_Back_Out_1': 'market_basket_name', 'Back_Out_1_Tier': 'confidence'}, axis = 1)
    bo_41 = pd.merge(bo_41, price_ref_table, on=['standardized_competitor_name', 'competitor_name', 'competitor_key', 'market_basket_name', 'confidence'])
    bo_41 = bo_41.rename({'market_basket_name': 'Item_to_Back_Out_1', 'confidence': 'Back_Out_1_Tier', 'menu_item_price_clean':'backout_1_price'}, axis = 1)
    # find back out items and their prices - second backout
    bo_42 = twobo_items[["standardized_competitor_name", "competitor_name", "competitor_key", "Item_to_Back_Out_2", "Back_Out_2_Tier"]]
    bo_42 = bo_42.rename({'Item_to_Back_Out_2': 'market_basket_name', 'Back_Out_2_Tier': 'confidence'}, axis=1, inplace = True)
    bo_42 = pd.merge(bo_42, price_ref_table, on = ['standardized_competitor_name', 'competitor_name', 'competitor_key', 'market_basket_name', 'confidence'])
    bo_42 = bo_42.rename({'market_basket_name': 'Item_to_Back_Out_2', 'confidence': 'Back_Out_2_Tier', 'menu_item_price_clean':'backout_2_price'}, axis = 1, inplace = True)
    #combine
    bo_4full = pd.merge(twobo_items, bo_41, on = ['standardized_competitor_name', 'competitor_name', 'competitor_key', 'Item_to_Back_Out_1', 'Back_Out_1_Tier'])
    twobo_final = pd.merge(bo_4full, bo_42, on = ['standardized_competitor_name', 'competitor_name', 'competitor_key', 'Item_to_Back_Out_2', 'Back_Out_2_Tier'])
    twobo_final['Ala_Carte_Price'] = round((twobo_final['menu_item_price_clean'] - twobo_final['backout_1_price'] - twobo_final['backout_2_price']).apply(pd.to_numeric), 2)
    twobo_final = twobo_final.drop(columns = ["backout_1_price", "backout_2_price"])
  else:
    twobo_final = pd.DataFrame()
  
  # 1 add on
  one_addon_item = table[(table.Operation=='NaN')  & (table.Item_to_Back_Out_1=='NaN') & (table.Item_to_Back_Out_2=='NaN') & (table.Item_to_Add_1!='NaN')]
  if len(one_addon_item) != 0:
    # find back out items and their prices
    add_1 = one_addon_item[["standardized_competitor_name", "competitor_name", "competitor_key", "Item_to_Add_1", "Add_1_Tier"]].drop_duplicates()
    add_1 = add_1.rename({'Item_to_Add_1':'market_basket_name', 'Add_1_Tier':'confidence'}, axis = 1)
    add_1 = pd.merge(add_1, price_ref_table, on = ['standardized_competitor_name', 'competitor_name', 'competitor_key', 'market_basket_name', 'confidence'])
    add_1 = add_1.rename({'market_basket_name': 'Item_to_Add_1', 'confidence': 'Add_1_Tier', 'menu_item_price_clean':'add_1_price'}, axis = 1)
    # combine
    oneaddon_final = pd.merge(one_addon_item, add_1, on = ['standardized_competitor_name', 'competitor_name', 'competitor_key', 'Item_to_Add_1', 'Add_1_Tier'])
    oneaddon_final['Ala_Carte_Price'] = round(oneaddon_final['menu_item_price_clean'] + oneaddon_final['add_1_price'], 2)
    oneaddon_final = oneaddon_final.drop(columns = ['add_1_price'])
  else:
    oneaddon_final = pd.DataFrame()
  
  final_table = pd.concat([normal_items, divide_items, divide_onebo_final, divide_twobo_final, onebo_final, twobo_final, oneaddon_final])

  return final_table
