# import find_parent

from dotenv import load_dotenv
from API_Connectors.aws_sql_connect import AWS_SQL, DummyData

from Get_All_Assets.Alpaca_assets import all_links_Alpaca
from Get_All_Assets.Binance_assets import all_links_Binance
from Get_All_Assets.Kraken_assets import all_links_Kraken

# establish connection to the Dummy Data DB
db_connection = DummyData(load_dotenv)

def insert_update_Kraken_rows():
    #get the data to insert
    Kraken_URIs = all_links_Kraken()
    # insert into DummyData.kraken_assets
    # insert into DummyData.assets

def insert_update_Binance_rows():
    #get the data to insert
    Binance_URIs = all_links_Binance()
    # inser into DummyData.binance_assets
    # insert into DummyData.assets

def insert_update_Aplaca_rows():
    #get the data to insert
    Alpaca_Assets = all_links_Alpaca()
    # insert into DummyData.alpaca_assets
    # insert into DummyData.assets
