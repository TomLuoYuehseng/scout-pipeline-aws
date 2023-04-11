from ETL_Lib import *


def dt_or_nor(row):  
    if not 'Downtown' in row['city']:
        return row['city'] + ' ' + row['State']
    else:
        return row['city']


def update_nielsen_data(nielsen_file_name,engine):

    """
    
    This function is used to update nielsen data.

    Inputs:
    nielsen_file_name (str) : The file name of the newest Nielsen data file.
                                e.g. 'Canteen Pricing Data 13 WE 06.11.2022.csv'

    """

    # nielsen_file_name = 'Canteen Pricing Data 13 WE 06.11.2022.csv'
    
    nielsen_raw = read_df_from_csv_on_s3(input_bucket='cdl-scout', input_key='Nielsen_Data/' + nielsen_file_name)
    marketbasket = read_excel_from_s3(Input_Bucket='cdl-scout', Input_Key='Nielsen_Data/CPG Data Mappings - Master.xlsx', Sheet_Name='CATEGORY MAPPING')
    locality = read_excel_from_s3(Input_Bucket='cdl-scout', Input_Key='Nielsen_Data/CPG Data Mappings.xlsx', Sheet_Name='REGION & STATE MAPPING')
    print(psutil.Process(os.getpid()).memory_info().rss / 1024 ** 2)

    locality = locality.rename(columns={'Unique Markets': 'temploc'}, inplace=False)
    locality['city'] = locality['temploc'].str.replace('.* - ', '')
    locality['Unique.Markets'] = locality.apply(lambda row: dt_or_nor(row), axis=1)
    locality = locality.drop('temploc', axis=1)

    nielsen_raw['UPC'] = nielsen_raw['UPC'].astype(str)
    nielsen_raw['UPC'] = nielsen_raw['UPC'].str.split('.').str[0]
    marketbasket['UPC'] = marketbasket['UPC'].astype(str)

    print(psutil.Process(os.getpid()).memory_info().rss / 1024 ** 2)
    marketbasket = marketbasket.rename(columns={"UPC Description":"UPC.Description", "Category":'Foodbuy.Category', "Sub Category":"Sub.Category", "Market Basket Name.1":"Market.Basket.Name.1",
                                                "Brand Low":"Brand.Low", "Base Size":"Base.Size", "Market Basket Name":"Market.Basket.Name", 'Canteen Item':'Canteen.Item',
                                                "Canteen Item.1":"Canteen.Item.1", "Note from Remove List":"Note.from.Remove.List", "New Item":"New.Item", "Sub Category.1":"Sub.Category.1",
                                                "Avg. 90th Percentile Price (National) w/ Exclusions":"Avg..90th.Percentile.Price..National..w..Exclusions"}, inplace=False)
    print('finish rename')
    nielsen_mb = pd.merge(nielsen_raw, marketbasket, on=['UPC'], how = 'left')
    print('finish merge')
    nielsen_mb = nielsen_mb.rename(columns={'Market Name': 'Unique.Markets'}, inplace=False)
    print('finish rename 2')
    print(nielsen_mb.columns)
    print(locality.columns)
    nielsen_full = pd.merge(nielsen_mb, locality, on=['Unique.Markets'])
    print(nielsen_full.head(10))
    overwrite_scoutdb_table_noindex(nielsen_full, 'AWS_PBI_Nielsen_Raw',engine)
    print(nielsen_full.size)
    print(psutil.Process(os.getpid()).memory_info().rss / 1024 ** 2)

# update_nielsen_data(nielsen_file_name = "Canteen Pricing Data 13 WE 06.11.2022.csv")
