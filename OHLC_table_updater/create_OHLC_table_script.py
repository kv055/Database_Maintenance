import find_parent
from dotenv import load_dotenv
from API_Connectors.aws_sql_connect import SQL_Server

from PriceData.Get_OHLC_Class import Import_OHLC_Data

# establish connection to the DB Server
DB_connection = SQL_Server('DummyData')
# querry all assets from DB
def return_all_asset_URLs():
    urls_to_fetch = []
    query =  f"SELECT * from DummyData.binance_assets"
    DB_connection.cursor.execute(query)
    table = DB_connection.cursor.fetchall()
    for row in table:
        urls_to_fetch.append({
                'URL':row[2],
                'Ticker': row[1],
                'Data_Provider': 'Binance'
            })
    return urls_to_fetch
all_asset_URLs = return_all_asset_URLs()
# Close connection
DB_connection.close()
# establish connection to the OHLC DB
DB_Server = SQL_Server('OHLC')
# Get all tables back
DB_Server.cursor.execute('SHOW TABLES')
all_tables = DB_Server.cursor.fetchall()
# Define config objects
all_configs = []
for element in all_tables:
    # for each table make a config object
    table_name_candle_size = element[0].decode()
    config = {
        'Candle_Size': table_name_candle_size,
        'Start_Time': None,
        'End_Time': None,
        'Data_Set_Size': None
    }
    all_configs.append(config)

# Get PriceData
for configuration in all_configs:
    Pricedata = Import_OHLC_Data(config)
    for index,asset in enumerate(all_asset_URLs):
        # Fetch the Price Data Sets from external API's
        Formated_OHLC_Data_Set = Pricedata.OHLC_Price_List_w_Metadata(asset)
        # fetch same asset from db by using Exchange, Ticker, CandleSize as PK
        # Join on TimeStamp
        # Compare entries with left/right joins
        join_sql = f"""

        """
        DB_Server.cursor.execute(join_sql)
        compare = DB_Server.cursor.fetchall()

        # if lücke zwischen frühesten Datum in neuer OHLC daten dann von dort aus früheres Datenset fetchen
        
        #enter into DB Table
        enter_OHLC_set_into_Table = f"""

        """
        DB_Server.cursor.execute()
        DB_Server.connection.commit()





# Close connection
DB_Server.close()