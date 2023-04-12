import pandas as pd
import psutil
import os
def dt_or_nor(row):  
    if not 'Downtown' in row['city']:
        return row['city'] + ' ' + row['State']
    else:
        return row['city']

def read_excel_from_s3(filename, Sheet_Name):
  # Input_Bucket = 'cdl-scout'
  # Input_Key = 'Internal_Pricing_Exclusions_Tiers.xlsx'
  # Sheet_Name = 'Internal - Prices Exclusion'
  df = pd.read_excel(filename, sheet_name=Sheet_Name)
  return df

def update_nielsen_data():
    
    nielsen_raw = pd.read_csv('/Users/yueshengluo/Desktop/scout-pipeline-aws/testfolder/Canteen Pricing Data 13 WE 06.11.2022.csv')
    marketbasket = read_excel_from_s3('/Users/yueshengluo/Desktop/scout-pipeline-aws/testfolder/CPG Data Mappings.xlsx', Sheet_Name='CATEGORY MAPPING')
    locality = read_excel_from_s3('/Users/yueshengluo/Desktop/scout-pipeline-aws/testfolder/CPG Data Mappings.xlsx', Sheet_Name='REGION & STATE MAPPING')
    locality = locality.rename(columns={'Unique Markets': 'temploc'}, inplace=False)
    locality['city'] = locality['temploc'].str.replace('.* - ', '',regex = True)
    #print(locality['city'])
    locality['Unique.Markets'] = locality.apply(lambda row: dt_or_nor(row), axis=1)
    locality = locality.drop('temploc', axis=1)

    nielsen_raw['UPC'] = nielsen_raw['UPC'].astype(str)
    nielsen_raw['UPC'] = nielsen_raw['UPC'].str.split('.').str[0]
    marketbasket['UPC'] = marketbasket['UPC'].astype(str)


    marketbasket = marketbasket.rename(columns={"UPC Description":"UPC.Description", "Category":'Foodbuy.Category', "Sub Category":"Sub.Category", "Market Basket Name.1":"Market.Basket.Name.1",
                                                "Brand Low":"Brand.Low", "Base Size":"Base.Size", "Market Basket Name":"Market.Basket.Name", 'Canteen Item':'Canteen.Item',
                                                "Canteen Item.1":"Canteen.Item.1", "Note from Remove List":"Note.from.Remove.List", "New Item":"New.Item", "Sub Category.1":"Sub.Category.1",
                                                "Avg. 90th Percentile Price (National) w/ Exclusions":"Avg..90th.Percentile.Price..National..w..Exclusions"}, inplace=False)

    nielsen_mb = pd.merge(nielsen_raw, marketbasket, on=['UPC'], how = 'left')
    nielsen_mb = nielsen_mb.rename(columns={'Market Name': 'Unique.Markets'}, inplace=False)
    nielsen_full = pd.merge(nielsen_mb, locality, on=['Unique.Markets'])
    #print(nielsen_mb['Unique.Markets'])
    nielsen_full.memory_usage(index=True).sum()
    print(nielsen_full.size)
    print(psutil.Process(os.getpid()).memory_info().rss / 1024 ** 2)

update_nielsen_data()