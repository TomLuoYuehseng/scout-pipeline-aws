from ETL_Lib import *  # noqa


rds_host = os.environ["database_host"]
user_name = os.environ["user_name"]
database_name = os.environ["database_name"]
database_port = os.environ["database_port"]
database_password = os.environ["database_password"]
def handler(event, context):
    s3_key_list = event["s3_key_list"]
    s3_filename_list = event["s3_filename_list"]
    print(database_name)
    print(rds_host)
    engine = initiate_rds_connection(rds_host, user_name, database_name, database_password, database_port)
    QA_Dahsboard_Data_Load(s3_key_list,s3_filename_list,engine)
    return

"""
standardized_competitor_name list: ["Au Bon Pain", "Blimpie", "Moe's Southwest Grill", "Tropical Smoothie Cafe", "Which Wich", "Friendly's", "Jimmy John's", "Shake Shack", "Bob Evans", "Potbelly", 
        "Red Robin", "Boston Market", "Chick-fil-A", "Chipotle", "Corner Bakery", "Denny's", "Five Guys", "IHOP", "Jersey Mike's Subs", "Lemonade", "Le Pain Quotidien", "Panera Bread", "Peet's Coffee"]

s3_key_list list: ["AuBonPain", "Blimpie", "MoesSouthwestGrill", "TropicalSmoothieCafe", "WhichWich", "Friendlys", "JimmyJohns", "ShakeShack", "BobEvans", "Potbelly", "RedRobin",
        "BostonMarket", "ChickFilA", "Chipotle", "CornerBakery", "Dennys", "FiveGuys", "Ihop", "JerseyMikes", "Lemonade", "LePainQuotidien", "PaneraBread", "PeetsCoffee"]

s3_filename_list list:["aubonpain", "blimpie", "moessouthwestgrill", "tropicalsmoothiecafe", "whichwich", "friendlys", "jimmy johns", "shakeshack", "bobevans", "potbelly", "redrobin", ]
        "bostonmarket", "chickfila", "chipotle", "cornerbakery", "dennys", "fiveguys", "ihop", "jerseymikes", "lemonade", "lepainquotidien", "panerabread", "peetscoffee"]

"""


def QA_Dahsboard_Data_Load(s3_key_list,s3_filename_list,engine):
    """
    
    Use this function to update Scout QA Dashboard with the newly scraped data and updated QA_Market_Basket_Conversion.csv file

    Inputs:
    s3_key_list (list) : A list S3 key names. 
                            e.g. s3_key_list = ['AuBonPain', "MoesSouthwestGrill"]
    s3_filename_list (list) : A list of S3 filenames
                                e.g. s3_filename_list = ['aubonpain', "moessouthwestgrill"]
    
    Sample Inputs:
    QA_Dahsboard_Data_Load(s3_key_list=["AuBonPain", "Blimpie", "MoesSouthwestGrill", "TropicalSmoothieCafe", "WhichWich"], 
                        s3_filename_list=['aubonpain', "blimpie, "moessouthwestgrill", 'tropicalsmoothiecafe', 'whichwich'])

    """

    # load market basket conversion file
    market_basket_main = read_df_from_csv_on_s3('cdl-scout', 'QA_Market_Basket_Conversion.csv')
    market_basket_main = market_basket_main.rename(columns={"Standardized_Competitor_Name": "standardized_competitor_name",
                                                            "Menu_Item_Name": "menu_item_name",
                                                            "Market_Basket_Name": "market_basket_name"}, inplace=False)

    # read in competitor files & generate PBI_Scout_QA_Dashboard table 
    files_list = []
    for key, filename in zip(s3_key_list, s3_filename_list):
        files = read_df_from_csv_on_s3('cdl-scout', "Pipline_Staging_Table/" + key + '/' + filename + '_staging.csv')
        files_list.append(files)
    competitor_files = pd.concat(files_list)

    qa_table = pd.merge(competitor_files, market_basket_main, on=['standardized_competitor_name', 'menu_item_name'])
    qa_table = qa_table.dropna(subset=['market_basket_name'])

    # update database
    table_name = "AWS_PBI_Scout_QA_Dashboard"
    overwrite_scoutdb_table_noindex(qa_table, table_name, engine)
    #query_result = query_all_data(table_name, engine)
    #print(query_result)