from ETL_Lib import *  # noqa
from Scout_Main_Update import *
from PBI_Data_Generators import *

rds_host = os.environ["database_host"]
user_name = os.environ["user_name"]
database_name = os.environ["database_name"]
database_port = os.environ["database_port"]
database_password = os.environ["database_password"]


def handler(event, context):
    competitor_list = event["competitor_list"]
    competitor_formal_name = event["competitor_formal_name"]
    print(database_name)
    print(rds_host)
    engine = initiate_rds_connection(rds_host, user_name, database_name, database_password, database_port)
    update_old_price(competitor_list, competitor_formal_name, engine)
    Scout_Production_Data_Load(engine)
    Zip_Competitor_Lookup(engine)
    Unit_Competitor_Lookup(engine)
    Competitor_Address_Lookup(engine)
