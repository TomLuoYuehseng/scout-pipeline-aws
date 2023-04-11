import pandas as pd
from datetime import date
from Parser_Function_Lib import * # noqa

"""

IMPORTANT NOTES:
!!!NEVER run this script on the Digital Ocean Server!!!
!!! ALWAYS Check S3 Bucket Name First !!! 

"""

"""
standardized_competitor_name list: ["Au Bon Pain", "Blimpie", "Moe's Southwest Grill", "Tropical Smoothie Cafe", "Which Wich", "Friendly's", "Jimmy John's", "Shake Shack", "Bob Evans", "Potbelly", 
        "Red Robin", "Boston Market", "Chick-fil-A", "Chipotle", "Corner Bakery", "Denny's", "Five Guys", "IHOP", "Jersey Mike's Subs", "Lemonade", "Le Pain Quotidien", "Panera Bread", "Peet's Coffee"]

s3_keyname list: ["AuBonPain", "Blimpie", "MoesSouthwestGrill", "TropicalSmoothieCafe", "WhichWich", "Friendlys", "JimmyJohns", "ShakeShack", "BobEvans", "Potbelly", "RedRobin",
        "BostonMarket", "ChickFilA", "Chipotle", "CornerBakery", "Dennys", "FiveGuys", "Ihop", "JerseyMikes", "Lemonade", "LePainQuotidien", "PaneraBread", "PeetsCoffee"]

s3_filename list:["aubonpain", "blimpie", "moessouthwestgrill", "tropicalsmoothiecafe", "whichwich", "friendlys", "jimmy johns", "shakeshack", "bobevans", "potbelly", "redrobin", ]
        "bostonmarket", "chickfila", "chipotle", "cornerbakery", "dennys", "fiveguys", "ihop", "jerseymikes", "lemonade", "lepainquotidien", "panerabread", "peetscoffee"]

"""


def Standardize_Scraped_Data(parsing_result, standardized_competitor_name, competitor_url, s3_keyname, s3_filename, output_path):

    """
    
    This function is used to standardized parsed data. It will also generate data for market basket mapping.

    Inputs:
    parsing_result (str) : The LOCAL file path to parsed competitor menu data
    standardized_competitor_name (str) : Full competitor name. 
                                            e.g. "Panera Bread", "Moe's Southwest Grill"
    competitor_url (str) : Home page of the competitor. 
                            e.g. "https://www.moes.com/"
    s3_keyname (str) : The S3 competitor folder name.
                        e.g. "MoesSouthwestGrill" 
    s3_filename (str) : In the format of lower case competitor name, no space in between. 
                        e.g. "moessouthwestgrill"
    output_path (str) : The local path where the data will be written to
    
    Sample Inputs:
    Standardize_Scraped_Data(parsing_result="/DataDrive/Data Science/Marcia/Repo/au_bon_parsed_jul18.csv", standardized_competitor_name="Au Bon Pain", 
                                competitor_url="https://www.aubonpain.com/", s3_keyname="AuBonPain", s3_filename="aubonpain", output_path="/DataDrive/Data Science/Marcia/Repo/aubonpain_data_updated.csv")

    """

    # Standardize scraped data
    competitor_table = pd.read_csv(parsing_result)
    standardized_name = standardized_competitor_name
    competitor_url = competitor_url
    timestamp = get_time_stamp()
    competitor_rank = ""

    competitor_table['menu_item_price_original'] = competitor_table['menu_item_price_original'].astype(str)
            
    competitot_format = formatting(competitor_table) # noqa
    competitot_format = format_data_key(df=competitot_format, # noqa
                                    standardized_name=standardized_name,
                                    competitor_url=competitor_url,
                                    scraped_date = int(timestamp + '01'),
                                    timestamp=timestamp,
                                    competitor_rank=competitor_rank) # noqa
    comptitor_finalized = competitot_format.reindex(columns=order_of_column) # noqa
    comptitor_finalized = comptitor_finalized[comptitor_finalized.menu_item_price_original != '0.0']
    
    currentdate = date.today().strftime("%Y%m%d")
    write_df_to_csv_on_s3(comptitor_finalized, 'cdl-scout', 'Pipline_Staging_Table/' + s3_keyname + "/" + s3_filename + '_staging_' + currentdate + '.csv')
    write_df_to_csv_on_s3(comptitor_finalized, 'cdl-scout', 'Pipline_Staging_Table/' + s3_keyname + "/" + s3_filename + '_staging.csv')

    # Generate simplified data for market basket mapping
    item_table = comptitor_finalized[['menu_item_name', 'menu_item_price_original']].drop_duplicates()
    item_table = item_table.loc[~(item_table['menu_item_price_original'] == 0)]
    item_table['standardized_competitor_name'] = standardized_competitor_name
    
    item_table.to_csv(output_path, index=False)

