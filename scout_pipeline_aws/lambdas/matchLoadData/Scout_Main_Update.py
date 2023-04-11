from numpy.core.numeric import NaN
from ETL_Lib import *  # noqa
import pandas as pd
import re


"""
IMPORTANT NOTES:
!!! NEVER run this script on Digital Ocean Server !!!

This script is used to update Scout database when competitor data got refreshed or a new competior is added 
To update Scout PowerBI database, please goto PBI_Data_Generators.py

"""

"""
competitor_formal_name list: ["Au Bon Pain", "Blimpie", "Moe's Southwest Grill", "Tropical Smoothie Cafe", "Which Wich", "Friendly's", "Jimmy John's", "Shake Shack", "Bob Evans", "Potbelly", 
        "Red Robin", "Boston Market", "Chick-fil-A", "Chipotle", "Corner Bakery", "Denny's", "Five Guys", "IHOP", "Jersey Mike's Subs", "Lemonade", "Le Pain Quotidien", "Panera Bread", "Peet's Coffee"]

competitor_list list: ["AuBonPain", "Blimpie", "MoesSouthwestGrill", "TropicalSmoothieCafe", "WhichWich", "Friendlys", "JimmyJohns", "ShakeShack", "BobEvans", "Potbelly", "RedRobin",
        "BostonMarket", "ChickFilA", "Chipotle", "CornerBakery", "Dennys", "FiveGuys", "Ihop", "JerseyMikes", "Lemonade", "LePainQuotidien", "PaneraBread", "PeetsCoffee"]

s3_filename list:["aubonpain", "blimpie", "moessouthwestgrill", "tropicalsmoothiecafe", "whichwich", "friendlys", "jimmy johns", "shakeshack", "bobevans", "potbelly", "redrobin", ]
        "bostonmarket", "chickfila", "chipotle", "cornerbakery", "dennys", "fiveguys", "ihop", "jerseymikes", "lemonade", "lepainquotidien", "panerabread", "peetscoffee"]

"""

competitor_list = ['PaneraBread']
competitor_formal_name = ['Panera Bread']

def assign_mb(competitor_list):
  """
  
  This function is for market basket mapping and further more standardize scraped data based on stakeholder's request
  
  Inputs:
  competiotr_list (list) : This is a list of updated competitors. Make sure the string is in the format of S3 competitor folder name
                           e.g. "Potbelly", "MoesSouthwestGrill"
  
  """
  
  # load market basket conversion file
  market_basket_main = read_df_from_csv_on_s3('cdl-scout', 'QA_Market_Basket_Conversion.csv') # noqa
  market_basket_main = market_basket_main.rename(columns={'Standardized_Competitor_Name': 'standardized_competitor_name',
                                                          "Menu_Item_Name":"menu_item_name",
                                                          "Market_Basket_Name":"market_basket_name"}, inplace=False)
  market_basket_main = market_basket_main.drop_duplicates()
  market_basket_main.dropna(subset=['market_basket_name'], inplace=True)
  market_basket_main['menu_item_name'] = [char.encode('ascii',errors='ignore') for char in market_basket_main['menu_item_name']]
  market_basket_main['menu_item_name'] = [char.decode() for char in market_basket_main['menu_item_name']]
  
  # load market region zip conversion
  market_region_zip = read_df_from_csv_on_s3('cdl-scout', '3-Digit_Zip_Markets.csv') # noqa
  market_region_zip.three_code_zip = market_region_zip.three_code_zip.astype('str')
  market_region_zip['three_code_zip'] = market_region_zip['three_code_zip'].apply(lambda x: x.zfill(3))
  
  parser_result_list = []
  for comp in competitor_list:
    # read compeleted parsing result from Pipeline_Staging_Table/, write into Scrape_Data/
    parsed_comp = read_df_from_csv_on_s3('cdl-scout', 'Pipline_Staging_Table/'+comp+'/'+comp.lower()+'_staging.csv') # noqa
    parser_result_list.append(parsed_comp)
  parser_result = pd.concat(parser_result_list)
  parser_result['menu_item_name'] = parser_result['menu_item_name'].str.strip()
  # parser_result['standardized_competitor_name'].drop_duplicates()
  
  # clean up postal code, remove any invalid zips (Canadian zip codes)
  parser_result.dropna(subset=['postal'], inplace=True)
  parser_result = parser_result[~parser_result.postal.astype(str).str.contains("[A-z]", na=False)]
  parser_result['postal'] = parser_result['postal'].astype(str).str.rsplit('-', n=1).str.get(0)
  parser_result.dropna(subset=['postal'], inplace=True)
  parser_result['num'] = parser_result['postal'].astype(str).str.len()

  parser_result = parser_result.query('num <= 5')
  parser_result['postal'] = parser_result['postal'].apply(lambda x: x.zfill(5))
  
  # assign competitor_key_id
  parser_result_id = create_competitor_key_id(parser_result) # noqa
  
  # clean up competitor data with non-ascii
  parser_result_id['menu_item_name'] = [char.encode('ascii',errors='ignore') for char in parser_result_id['menu_item_name']]
  parser_result_id['menu_item_name'] = [char.decode() for char in parser_result_id['menu_item_name']]
  parser_result_id = parser_result_id.drop_duplicates()
  # parser_result['three_code_zip'] = parser_result['postal'].apply(lambda x: x[0:3])
  # parser_result_id.to_csv('/DataDrive/Data Science/Marcia/Re_Opening_Guide/LTG_Scout_Competitor_Raw.csv', index = False)
  
  merge_mb = pd.merge(parser_result_id, market_basket_main, on=['standardized_competitor_name', 'menu_item_name'])
  merge_mb1 = merge_mb.drop_duplicates(subset=['menu_item_name', 'menu_item_price_clean', 'competitor_key'])
  
  # calculate min & max price, count unique_location_presence, change column name, and assign missing columns
  merge_mb1["min_price"] = merge_mb1.groupby(['standardized_competitor_name', 'market_basket_name', 'menu_item_name'])['menu_item_price_clean'].transform('min')
  merge_mb1["max_price"] = merge_mb1.groupby(['standardized_competitor_name', 'market_basket_name', 'menu_item_name'])['menu_item_price_clean'].transform('max')
  merge_mb1["unique_location_presence"] = merge_mb1.groupby(["standardized_competitor_name", "market_basket_name", "menu_item_name"])["competitor_key"].transform('nunique')
  merge_mb1["existed_in_old_dataset"] = "NA"
  merge_mb1["menu_header"] = "NA"
  merge_mb1["menu_header_id"] = "NA"
  merge_mb1["menu_item_id"] = "NA"
  merge_mb1['three_code_zip'] = merge_mb1['postal'].apply(lambda x: x[0:3])
  merge_mb1 = merge_mb1.rename({"OPERATION":"Operation", "DIGIT":"Digit", "TIER_NUMBER":"confidence", "menu_item_des":"menu_item_description"}, axis=1)

  # assign market
  with_market = pd.merge(merge_mb1, market_region_zip, on=['three_code_zip'])
  with_market = with_market[['standardized_competitor_name', 'competitor_name', 'competitor_key', 'competitor_key_id', 'competitor_url', 'three_code_zip', 'Market', 'Region',
                             'menu_header', 'menu_header_id','menu_item_name', 'menu_item_id', 'menu_item_description', 'menu_item_price_original', 
                             'menu_item_price_clean', 'market_basket_name', 'confidence', 'min_price', 'max_price', 'existed_in_old_dataset', 
                             'Item_to_Back_Out_1', 'Back_Out_1_Tier', 'Item_to_Back_Out_2', 'Back_Out_2_Tier', 'Item_to_Add_1', 'Add_1_Tier', 'Operation', 'Digit','RANK','unique_location_presence', 
                             'competitor_address', 'competitor_source_lat', 'competitor_source_lng', 'postal', 'street_number', 'street', 'city', 'state', 'country']]
  
  # subset data to only include minimum tire (confidence)
  with_market["min_tire"] = with_market.groupby(["standardized_competitor_name", "competitor_name", "competitor_key", "competitor_key_id", "market_basket_name", 
                                                 "competitor_source_lat", "competitor_source_lng", "postal"])["confidence"].transform('min')

  tire_exclusion = with_market.loc[with_market['confidence'] == with_market["min_tire"]]
  tire_exclusion = tire_exclusion.drop(columns = ['min_tire'])
  tire_exclusion["menu_item_price_clean"] = tire_exclusion["menu_item_price_clean"].astype(float)
  
  # mannuly clean up some catering menus
  for_bo = tire_exclusion.drop(tire_exclusion[(tire_exclusion["standardized_competitor_name"]=="Bob Evans") & (tire_exclusion["market_basket_name"]=="French Fries") & (tire_exclusion["menu_item_price_clean"] > 5)].index)
  for_bo = for_bo.drop(for_bo[(for_bo["standardized_competitor_name"]=="Chick-fil-A") & (for_bo["market_basket_name"]=="Bottled Soda (20 oz.)") & (for_bo["menu_item_price_clean"] < 1.75)].index)
  for_bo = for_bo.drop(for_bo[(for_bo["standardized_competitor_name"]=="Chick-fil-A") & (for_bo["market_basket_name"]=="Fountain 32 oz. (Large)") & (for_bo["menu_item_price_clean"] < 1.65)].index)
  for_bo = for_bo.drop(for_bo[(for_bo["standardized_competitor_name"]=="Chick-fil-A") & (for_bo["market_basket_name"]=="Fresh Fruit Cup") & (for_bo["menu_item_price_clean"] < 2)].index)
  for_bo = for_bo.drop(for_bo[(for_bo["standardized_competitor_name"]=="Chick-fil-A") & (for_bo["market_basket_name"]=="Side Salad") & (for_bo["menu_item_price_clean"] < 5)].index)
  for_bo = for_bo.drop(for_bo[(for_bo["standardized_competitor_name"]=="Chick-fil-A") & (for_bo["market_basket_name"]=="Soup (12 oz. / cup)") & (for_bo["menu_item_price_clean"] < 5)].index)
  for_bo = for_bo.drop(for_bo[(for_bo["standardized_competitor_name"]=="Chick-fil-A") & (for_bo["market_basket_name"]=="Yogurt Parfait") & (for_bo["menu_item_price_clean"] < 5)].index)
  for_bo = for_bo.drop(for_bo[(for_bo["standardized_competitor_name"]=="Bob Evans") & (for_bo["market_basket_name"]=="Side Vegetable") & (for_bo["menu_item_price_clean"] > 5)].index)
  for_bo = for_bo.drop(for_bo[(for_bo["standardized_competitor_name"]=="Bob Evans") & (for_bo["market_basket_name"]=="Breakfast Entree (2 Eggs, Protein, and Potatoes)") & (for_bo["menu_item_price_clean"] > 15)].index)
  for_bo = for_bo.drop(for_bo[(for_bo["standardized_competitor_name"]=="IHOP") & (for_bo["market_basket_name"]=="French Fries") & (for_bo["menu_item_price_clean"] > 3)].index)
  for_bo = for_bo.drop(for_bo[(for_bo["standardized_competitor_name"]=="IHOP") & (for_bo["market_basket_name"]=="Biscuit (each)") & (for_bo["menu_item_price_clean"] > 5)].index)
  for_bo = for_bo.drop(for_bo[(for_bo["standardized_competitor_name"]=="IHOP") & (for_bo["market_basket_name"]=="Wrap") & (for_bo["menu_item_price_clean"] < 7)].index)
  for_bo = for_bo.drop(for_bo[(for_bo["standardized_competitor_name"]=="Five Guys") & (for_bo["market_basket_name"]=="French Fries") & (for_bo["menu_item_price_clean"] > 5)].index)
  for_bo = for_bo.drop(for_bo[(for_bo["standardized_competitor_name"]=="Blimpie") & (for_bo["market_basket_name"]=="'Bottled Soda (20 oz.)") & (for_bo["menu_item_price_clean"] > 5)].index)
  for_bo = for_bo.drop(for_bo[(for_bo["standardized_competitor_name"]=="Blimpie") & (for_bo["market_basket_name"]=="Bottled Water (20 oz.)") & (for_bo["menu_item_price_clean"] > 5)].index)
  for_bo = for_bo.drop(for_bo[(for_bo["standardized_competitor_name"]=="Jersey Mike's Subs") & (for_bo["market_basket_name"]=="Bottled Juice (12 oz.)") & (for_bo["menu_item_price_clean"] > 5)].index)
  for_bo = for_bo.drop(for_bo[(for_bo["standardized_competitor_name"]=="Jersey Mike's Subs") & (for_bo["market_basket_name"]=="Wrap") & (for_bo["menu_item_price_clean"] > 19)].index)
  for_bo = for_bo.drop(for_bo[(for_bo["standardized_competitor_name"]=="Jersey Mike's Subs") & (for_bo["market_basket_name"]=="Add Avocado") & (for_bo["menu_item_price_clean"] > 5)].index)
  for_bo = for_bo.drop(for_bo[(for_bo["standardized_competitor_name"]=="Tropical Smoothie Cafe") & (for_bo["market_basket_name"]=="Bag Chip (i.e. Frito Lay)") & (for_bo["menu_item_price_clean"] > 15)].index)
  # for_bo['standardized_competitor_name'].drop_duplicates()
  
  # back-out & add-on items
  standardized_table = back_out_logic(for_bo) # noqa
  # standardized_table.to_csv('/DataDrive/Data Science/Marcia/Repo/compassdigital.ScOUT/Scout_Pipeline/ETL/bo_test3.csv')

  
  return standardized_table


def update_old_price(competitor_list, competitor_formal_name,engine):
  """
  
  Use this function to overwrite Scout database with the newest data and write the old Scout data onto S3
  
  Inputs:
  competitor_formal_name (list) : This is a list of standardized competitor name
                           e.g. ["Potbelly", "Moe's Southwest Grill", "Chick-fil-A"]
  
  """

  timestamp = grab_time_stamp()
  updated_table = assign_mb(competitor_list)
  # updated_table.to_csv('/DataDrive/Data Science/Marcia/Repo/compassdigital.ScOUT/Scout_Pipeline/ETL/bo_test.csv')
  print(updated_table['standardized_competitor_name'].drop_duplicates())
  # temp solution as compeletely replace the old data
  # future solution will be -- depends on the timestamp, if exist one year, remove, if not, keep -- for missing loactions/items in new data
  query = 'Select * from National_Competitor_Prices_Labelled_Revised_2'
  old_scout_data = scoutdb_tables(query,engine)
  ####### Don't really need to run this line if we are not updating/adding any competitor's data
  write_df_to_csv_on_s3(old_scout_data, output_bucket='cdl-scout', output_key='Competitor_Price_Movement/AWS_National_Competitor_Prices_' + timestamp + '.csv')

  # subset competitors
  remaining_data = old_scout_data[~old_scout_data['standardized_competitor_name'].isin(competitor_formal_name)]
  remaining_data['standardized_competitor_name'].drop_duplicates()

  # attach new data
  remaining_data = remaining_data.reset_index(drop=True)
  updated_table = updated_table.reset_index(drop=True)
  new_scout_data = pd.concat([remaining_data, updated_table])
  new_scout_data = new_scout_data.drop('level_0', axis = 1)

  # update database
  overwrite_scoutdb_table(result_table=new_scout_data, db_table_name='AWS_National_Competitor_Prices_Labelled_Revised_2', engine = engine)


# competitor_list = ["MoesSouthwestGrill", "PaneraBread"]
# competitor_formal_name = ["Moe's Southwest Grill", "Panera Bread"]
