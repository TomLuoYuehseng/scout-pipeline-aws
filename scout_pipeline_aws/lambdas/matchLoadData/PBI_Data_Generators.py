from ETL_Lib import *  # noqa

"""
standardized_competitor_name list: ["Au Bon Pain", "Blimpie", "Moe's Southwest Grill", "Tropical Smoothie Cafe", "Which Wich", "Friendly's", "Jimmy John's", "Shake Shack", "Bob Evans", "Potbelly", 
        "Red Robin", "Boston Market", "Chick-fil-A", "Chipotle", "Corner Bakery", "Denny's", "Five Guys", "IHOP", "Jersey Mike's Subs", "Lemonade", "Le Pain Quotidien", "Panera Bread", "Peet's Coffee"]

s3_key_list list: ["AuBonPain", "Blimpie", "MoesSouthwestGrill", "TropicalSmoothieCafe", "WhichWich", "Friendlys", "JimmyJohns", "ShakeShack", "BobEvans", "Potbelly", "RedRobin",
        "BostonMarket", "ChickFilA", "Chipotle", "CornerBakery", "Dennys", "FiveGuys", "Ihop", "JerseyMikes", "Lemonade", "LePainQuotidien", "PaneraBread", "PeetsCoffee"]

s3_filename_list list:["aubonpain", "blimpie", "moessouthwestgrill", "tropicalsmoothiecafe", "whichwich", "friendlys", "jimmy johns", "shakeshack", "bobevans", "potbelly", "redrobin", ]
        "bostonmarket", "chickfila", "chipotle", "cornerbakery", "dennys", "fiveguys", "ihop", "jerseymikes", "lemonade", "lepainquotidien", "panerabread", "peetscoffee"]

"""


def Scout_Production_Data_Load(engine):
    """
    
    This function is used to generate the PBI table PBI_Scout_Raw_Data

    Inputs:
    None

    """
    query = "Select standardized_competitor_name, competitor_key_id, market_basket_name, menu_item_name, confidence, ala_carte_price, Item_to_Back_Out_1, Item_to_Back_Out_2 From National_Competitor_Prices_Labelled_Revised_2"
    competitor_full = scoutdb_tables(query,engine)
    competitor_full = competitor_full.rename(columns={'Item_to_Back_Out_1':'item_to_back_out', 'Item_to_Back_Out_2':'item_to_back_out_2'})
    competitor_full.dropna(subset=['market_basket_name'], inplace=True)
    competitor_full['market_basket_category'] = ''
    competitor_full.loc[competitor_full.item_to_back_out == 'NaN', 'item_to_back_out'] = ''
    competitor_full.loc[competitor_full.item_to_back_out_2 == 'NaN', 'item_to_back_out_2'] = ''

    overwrite_scoutdb_table_noindex(competitor_full, 'AWS_PBI_Scout_Raw_Data',engine)


def Zip_Competitor_Lookup(engine):
    """
    
    This function is used to generate the PBI table PBI_ZIP_Competitor_Lookup

    Inputs:
    None

    """

    query = '''Select distinct ZIP, standardized_competitor_name, competitor_key, competitor_key_id From Regional_Benchmarking_UCA'''
    geo_query = '''Select distinct competitor_key, competitor_source_lat, competitor_source_lng, three_code_zip, city, state, Region from National_Competitor_Prices_Labelled_Revised_2'''
    
    PBI_ZIP_Competitor_Lookup = scoutdb_tables(query,engine)
    comp_geo_info = scoutdb_tables(geo_query,engine)

    PBI_ZIP_Competitor_Lookup_full = pd.merge(PBI_ZIP_Competitor_Lookup, comp_geo_info, on = 'competitor_key', how = 'left')
    PBI_ZIP_Competitor_Lookup_full.dropna(subset=['three_code_zip'], inplace=True)

    overwrite_scoutdb_table_noindex(PBI_ZIP_Competitor_Lookup_full, 'AWS_PBI_ZIP_Competitor_Lookup',engine)


def Unit_Competitor_Lookup(engine):
    """
    
    This function is used to generate the PBI table PBI_Unit_Competitor_Lookup

    Inputs:
    None

    """

    query = "Select distinct UNIT_KEY, standardized_competitor_name, competitor_key, competitor_key_id From Unit_Regional_Benchmarking_UCA"
    contract_query = "Select distinct ERP_ENTITY_ID, ORG_UNIT_NAME from Scout_All_Contract_Tables"

    unit_names = scoutdb_tables(contract_query,engine)

    PBI_Unit_Competitor_Lookup = scoutdb_tables(query,engine)
    PBI_Unit_Competitor_Lookup = PBI_Unit_Competitor_Lookup.rename(columns = {"UNIT_KEY":"ERP_ENTITY_ID"})

    PBI_Unit_Competitor_Lookup = pd.merge(unit_names, PBI_Unit_Competitor_Lookup, on = 'ERP_ENTITY_ID')

    overwrite_scoutdb_table_noindex(PBI_Unit_Competitor_Lookup, 'AWS_PBI_Unit_Competitor_Lookup',engine)


def Competitor_Address_Lookup(engine):
    """
    
    This function is used to generate PBI table PBI_Scout_Competitor_Adress_Lookup

    Inputs:
    None

    """

    query = "select distinct standardized_competitor_name, competitor_name, competitor_key, competitor_key_id, competitor_address, city, state, postal from National_Competitor_Prices_Labelled_Revised_2"
    competitor_address_full = scoutdb_tables(query,engine)
    competitor_address_full.dropna(subset=['competitor_address'], inplace=True)

    # competitor_address_full.to_csv('/DataDrive/Data Science/Marcia/Repo/compassdigital.ScOUT/Scout_Pipeline/ETL/test.csv', index = False)

    overwrite_scoutdb_table_noindex(competitor_address_full, 'AWS_PBI_Scout_Competitor_Adress_Lookup',engine)

