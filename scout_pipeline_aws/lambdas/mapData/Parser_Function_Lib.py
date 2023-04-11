from lib2to3.pgen2.pgen import DFAState
import pandas as pd
import numpy as np
import boto3
import io
import hashlib
import datetime as dt
import os
import re


def initiate_client():
    """
    Establishes a boto3 client with the global credentials

    Input:
    none

    Return:
    actor (botor3.client) : boto3 client object
    """
    actor = boto3.client('s3')
    return actor


def get_time_stamp():
    client = initiate_client()
    timestampfile = client.list_objects_v2(Bucket='cdl-scout',
                                            Delimiter="/",
                                            Prefix="Temp_Page_Source/")
    for i in timestampfile["Contents"]:
        if '.txt' in str(i):
            timestamp = i["Key"].replace("Temp_Page_Source/", '').replace('.txt', '') # noqa
    return timestamp


def priceclean(x):
    try:
        x = float(x)
    except Exception:
        x = np.nan
    return(x)


def formatting(df):
    df["menu_item_price_clean"] = df["menu_item_price_original"].apply(
        lambda x: x.replace('(', '').replace(')', '').replace('$', '')
        .replace('+', '').replace('-', ''))

    df["menu_item_price_clean"] = df["menu_item_price_clean"].apply(
        lambda x: priceclean(x))
    table = df[~df["menu_item_price_clean"].isnull()]
    return(table)



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


def format_competitor_name(df, standardized_name):
    df["standardized_competitor_name"] = standardized_name
    table = create_competitor_key(df)
    return (table)


def create_competitor_key(df):
    key_val = df['competitor_name'].astype(str)
    df['competitor_key'] = [hashlib.md5(
        val.encode('utf-8')).hexdigest() for val in key_val]
    return (df)


def format_date(df, scraped_date):
    df['date'] = scraped_date
    df['date'] = pd.to_datetime(df['date'], format='%Y%m%d')
    table = create_date_key(df)
    table['date_last_modified'] = dt.datetime.today().strftime('%Y-%m-%d')
    return (table)


def create_date_key(df):
    key_val = df['target_id'].astype(str)+df['date'].astype(str)
    df['date_key'] = [hashlib.md5(val.encode(
        'utf-8')).hexdigest() for val in key_val]
    return(df)


def create_address_key(df, competitor_url):
    key_val = df['competitor_address'].astype(
        str)+df['postal'].astype(str)+df['competitor_name'].astype(str)
    df['address_key'] = [hashlib.md5(val.encode(
        'utf-8')).hexdigest() for val in key_val]
    df['country'] = 'United States of America'
    df['competitor_url'] = competitor_url
    return (df)


def create_menu_item_key(df):
    key_val = df['menu_item_name']+df['competitor_name'].astype(str) + \
        df['menu_item_cal'].astype(str)
    df['menu_item_key'] = [hashlib.md5(
        val.encode('utf-8')).hexdigest() for val in key_val]
    return(df)


def format_tags(df):
    df["tags"] = ""
    df["competitor_on_browser"] = ""
    df["competitor_on_mobile"] = ""
    df["competitor_has_delivery"] = ""
    df["competitor_has_pickup"] = ""
    df["competitor_food_score"] = ""
    df["competitor_star_rating"] = ""
    df["competitor_number_of_ratings"] = ""
    return(df)


def format_data_key(df, standardized_name, scraped_date, competitor_url, competitor_rank, timestamp):
    df = format_competitor_name(df, standardized_name)

    df = format_date(df, scraped_date)

    df = create_menu_item_key(df)

    df = create_address_key(df, competitor_url)

    df = format_tags(df)

    df["RANK"] = competitor_rank

    df["timestamp"] = timestamp


    return df



order_of_column = [
    "menu_item_name",
    "menu_item_price_original",
    "menu_item_price_clean",
    "menu_item_des",
    "menu_item_cal",
    "menu_category",
    "competitor_address",
    "city",
    "region",
    "state",
    "country",
    "street",
    "street_number",
    "postal",
    "competitor_source_lat",
    "competitor_source_lng",
    "target_id",
    "date",
    "date_last_modified",
    "standardized_competitor_name",
    "competitor_name",
    "competitor_key",
    "date_key",
    "menu_item_key",
    "address_key",
    "competitor_url",
    "competitor_on_browser",
    "competitor_on_mobile",
    "competitor_has_delivery",
    "competitor_has_pickup",
    "competitor_food_score",
    "competitor_star_rating",
    "competitor_number_of_ratings",
    "tags",
    "RANK",
    "timestamp"
]
