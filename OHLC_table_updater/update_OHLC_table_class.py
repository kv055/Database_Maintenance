import find_parent

from API_Connectors.aws_sql_connect import SQL_Server
from PriceData.Get_OHLC_Class import Import_OHLC_Data

class update_asset_table:
    def __init__(self):
        # Get all listed Asset URL's from DB
        self.all_asset_URLs = self.return_all_asset_URLs()
         # establish connection to the OHLC DB
        self.DB_Server = SQL_Server('OHLC')
        # # create '1D table if not exists
        # self.DB_Server.cursor.execute("CREATE TABLE IF NOT EXISTS 1_Day")
        # self.DB_Server.connection.commit()

        # create new_ohlc_temp_table if not exists
        self.DB_Server.cursor.execute("CREATE TABLE IF NOT EXISTS new_ohlc_temp_table LIKE 1_Day")
        self.DB_Server.connection.commit()

        # Get all tables back
        self.DB_Server.cursor.execute('SHOW TABLES')
        all_tables = self.DB_Server.cursor.fetchall()
        # Define config objects
        self.all_configs = []
        for element in all_tables:
            table_name_candle_size = element[0].decode()
            if table_name_candle_size != 'new_ohlc_temp_table':
                # for each table make a config object
                config = {
                    'Candle_Size': table_name_candle_size,
                    'Start_Time': None,
                    'End_Time': None,
                    'Data_Set_Size': None
                }
                self.all_configs.append(config)


    def return_all_asset_URLs(self):
        # establish connection to the DB Server
        DB_connection = SQL_Server('DummyData')
        
        # querry all assets from DB
        urls_to_fetch = []
        query =  f"""
            SELECT * from DummyData.assets
            WHERE data_provider = 'Kraken'
        """
        DB_connection.cursor.execute(query)
        table = DB_connection.cursor.fetchall()
        DB_connection.close()

        # formate and return the data
        for row in table:
            urls_to_fetch.append({
                'Data_Provider': row[0],
                'Ticker': row[1],
                'URL':row[2],
            })
        return urls_to_fetch

    def fetch_OHLC_from_API_and_update_table(self):
        # Get PriceData
        for configuration in self.all_configs:
            Pricedata = Import_OHLC_Data(configuration)
            for index,asset in enumerate(self.all_asset_URLs):
                # Fetch the Price Data Sets from external API's
                Formated_OHLC_Data_Set = Pricedata.OHLC_Price_List_for_DB(asset)
                # delete old Data_set
                self.DB_Server.cursor.execute("DELETE FROM new_ohlc_temp_table")
                self.DB_Server.connection.commit()
                # insert it into the temp rable
                insert_sql = "INSERT INTO new_ohlc_temp_table (Date, Open, High, Low, Close, Data_Provier, Ticker) VALUES (%s,%s,%s,%s,%s,%s,%s)"
                self.DB_Server.cursor.executemany(insert_sql, Formated_OHLC_Data_Set)
                self.DB_Server.connection.commit()
                # fetch same asset from db by using Exchange, Ticker, CandleSize as PK
                # Join on TimeStamp
                # Compare entries with left/right joins
                count_sql = f"""
                    select * from new_ohlc_temp_table
                    left join 1_Day on new_ohlc_temp_table.Date = 1_Day.Date
                    where 1_Day.Date is null
                """
                self.DB_Server.cursor.execute(count_sql)
                new_rows = self.DB_Server.cursor.fetchall()

                join_sql = f"""
                    insert into 1_Day
                        select new_ohlc_temp_table.* from new_ohlc_temp_table
                        left join 1_Day on new_ohlc_temp_table.Date = 1_Day.Date
                        -- where 1_Day.Date is null
                """
                self.DB_Server.cursor.execute(join_sql)
                self.DB_Server.connection.commit()
                print(index, asset['Data_Provider'], asset['Ticker'])




        # Close connection
        self.DB_Server.close()