# import find_parent

from dotenv import load_dotenv
from API_Connectors.aws_sql_connect import AWS_SQL, DummyData

from Get_All_Assets.Alpaca_assets import all_links_Alpaca
from Get_All_Assets.Binance_assets import all_links_Binance
from Get_All_Assets.Kraken_assets import all_links_Kraken

class update_asset_table:
    """
        Insert URL Data into Temp Table
         > left join temp & asset table = new data
         > inner join temp & asset table = update data
         > right join temp & asset table = delete data
    """
    def __init__(self):
        # establish connection to the Dummy Data DB
        self.db_connection = DummyData(load_dotenv)
        self.Alpaca_URL_List = all_links_Alpaca
        self.Binance_URL_List = all_links_Binance
        self.Kraken_URL_List = all_links_Kraken
    
    def set_Alpaca_URL_List(self):
        self.data_set =  self.Alpaca_URL_List()
        self.data_provider = 'Alpaca'

    def set_Binance_URL_List(self):
        self.data_set =  self.Binance_URL_List()
        self.data_provider = 'Binance'

    def set_Kraken_URL_List(self):
        self.data_set =  self.Kraken_URL_List()
        self.data_provider = 'Kraken'

    def enter_into_db(self):
        # create temp table to hold all new data 
        self.db_connection.cursor.execute("CREATE TABLE IF NOT EXISTS new_urls LIKE assets")
        self.db_connection.connection.commit()
        # no clue what that does
        self.db_connection.cursor.execute("DELETE FROM new_urls WHERE id")
        self.db_connection.connection.commit()
        # insert all new records into the temp table
        insert_sql = "INSERT INTO new_urls (ticker, data_provider, historical_data_url, live_data_url) VALUES (%s,%s,%s,%s)"
        self.db_connection.cursor.executemany(insert_sql, self.data_set)
        self.db_connection.connection.commit()

        # check if the assets table has records that are not in the temp table
        get_number_of_delisted_assets_sql = f"""
            select count(*) from new_urls
            right join binance_assets on new_urls.ticker = binance_assets.ticker
            where new_urls.id is null;
                
        """
        self.db_connection.cursor.execute(get_number_of_delisted_assets_sql)
        number_of_delisted_assets = self.db_connection.cursor.fetchall()

        if number_of_delisted_assets > 0:
            # get asset table records that are not in the temp table
            get_PK_from_delisted_assets_sql = f"""
                select * from new_urls
                right join binance_assets on new_urls.ticker = binance_assets.ticker
                where new_urls.id is null;   
            """
            self.db_connection.cursor.execute(get_PK_from_delisted_assets_sql)
            PK_from_delisted_assets = self.db_connection.cursor.fetchall()
            # delete rows that are not in the temp table
            for row in PK_from_delisted_assets:
                ticker = row[6]
                delete_delisted_row_sql = f"""
                    DELETE FROM binance_assets WHERE ticker = '{ticker}'
                """
                self.db_connection.cursor.execute(delete_delisted_row_sql)
                self.db_connection.connection.commit()

        # check if the temp table has records that are not in the assets table
        get_number_of_newly_listed_assets_sql = f"""
            select count(*) from new_urls
            left join binance_assets on new_urls.ticker = binance_assets.ticker
            where binance_assets.live_data_url <> new_urls.live_data_url 
                or binance_assets.id is null;
        """
        self.db_connection.cursor.execute(get_number_of_newly_listed_assets_sql)
        number_of_newly_listed_assets = self.db_connection.cursor.fetchall()

        if number_of_newly_listed_assets > 0:
            insert_newly_listed_assets_sql = f"""
                    insert into kraken_assets
                select 
                null, new_urls.ticker, new_urls.historical_data_url, new_urls.live_data_url 
                from new_urls
                left join kraken_assets on new_urls.ticker = kraken_assets.ticker
                where kraken_assets.live_data_url <> new_urls.live_data_url 
                    or kraken_assets.id is null ;
            """
            self.db_connection.cursor.executemany(insert_newly_listed_assets_sql)
            self.db_connection.connection.commit()

test_run = update_asset_table()
test_run.set_Alpaca_URL_List()
# test_run.enter_into_db()
test_run.set_Binance_URL_List()
# test_run.enter_into_db()
test_run.set_Kraken_URL_List()
# test_run.enter_into_db()
l=0