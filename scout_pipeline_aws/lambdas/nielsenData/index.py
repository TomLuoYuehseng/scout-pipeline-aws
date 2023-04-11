from ETL_Lib import *  # noqa
from Nielsen_CPG import *

rds_host = os.environ["database_host"]
user_name = os.environ["user_name"]
database_name = os.environ["database_name"]
database_port = os.environ["database_port"]
database_password = os.environ["database_password"]


def handler(event, context):
    nielsen_file_name = event["nielsen_file_name"]
    print(database_name)
    print(rds_host)
    engine = initiate_rds_connection(
        rds_host, user_name, database_name, database_password, database_port)
    update_nielsen_data(nielsen_file_name,engine)

