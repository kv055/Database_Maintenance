import find_parent
from API_Connectors.aws_sql_connect import SQL_Server
from Assets_table_updater.get_all_asset_urls_from_API import all_listed_assets


class update_asset_table:
    """
        Insert URL Data into Temp Table
         > left join temp & asset table = new data
         > inner join temp & asset table = update data
         > right join temp & asset table = delete data
    """
    def __init__(self):
        # establish connection to the Dummy Data DB
        self.db_connection = SQL_Server('DummyData')
        # Create Assets Table if it doesnts exist
        create_assets_table_sql = f"""
            CREATE TABLE IF NOT EXISTS `assets` (
            `data_provider` varchar(255) NOT NULL,
            `ticker` varchar(45) NOT NULL,
            `historical_data_url` varchar(255) DEFAULT NULL,
            `historical_data_req_body` varchar(255) DEFAULT NULL,
            `live_data_url` varchar(255) DEFAULT NULL,
            `live_data_req_body` varchar(255) DEFAULT NULL,
            `first_available_datapoint` DATETIME NOT NULL,
            `last_available_datapoint` DATETIME NOT NULL,
            PRIMARY KEY (`data_provider`,`ticker`)
            )
        """
        self.db_connection.cursor.execute(create_assets_table_sql)
        self.db_connection.connection.commit()

        # LOGIC ERROR TO BE FIXED, BUT WORKS FOR NOW AS LONG AS THE assets TABLE
        # DOESNT GET DELETED
        # if create_assets_table_sql gets executed then there will be an error
        # thrown once the delete_id_column_sql gets executed
        # beacause the nonexistend id Column

        # Delete id column if table exists
        delete_id_column_sql = f"""
            ALTER TABLE `DummyData`.`assets` 
            DROP COLUMN `id`,
            DROP INDEX `id_UNIQUE` ;
            ;
        """
        self.db_connection.cursor.execute(delete_id_column_sql)
        self.db_connection.connection.commit()
        
        # this statement would fix the above mentioned error but i
        # cant figure out the correct syntax

        # create_table_or_delete_id_column_sql_sql = f"""
        #     IF NOT EXISTS(
        #     CREATE TABLE `assets` (
        #         `data_provider` varchar(255) NOT NULL,
        #         `ticker` varchar(45) NOT NULL,
        #         `historical_data_url` varchar(255) DEFAULT NULL,
        #         `historical_data_req_body` varchar(255) DEFAULT NULL,
        #         `live_data_url` varchar(255) DEFAULT NULL,
        #         `live_data_req_body` varchar(255) DEFAULT NULL,
        #         PRIMARY KEY (`data_provider`, `ticker`)
        #     ) ELSE
        #     ALTER TABLE `DummyData`.`assets` 
        #         DROP COLUMN `id`,
        #         DROP INDEX `id_UNIQUE`;
        #     END IF;
        # """
        # self.db_connection.cursor.execute(create_table_or_delete_id_column_sql_sql)
        # self.db_connection.connection.commit()

        assets_api_instance = all_listed_assets()
        self.Alpaca_URL_List = assets_api_instance.all_links_Alpaca
        self.Binance_URL_List = assets_api_instance.all_links_Binance
        self.Kraken_URL_List = assets_api_instance.all_links_Kraken
    
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
        # # create temp table to hold all new data 
        # self.db_connection.cursor.execute("CREATE TEMPORARY TABLE temporary_new_assets LIKE assets")
        # self.db_connection.connection.commit()
        
        # create temp table to hold all new data 
        self.db_connection.cursor.execute("CREATE TABLE IF NOT EXISTS new_urls LIKE assets")
        self.db_connection.connection.commit()
        # no clue what that does
        self.db_connection.cursor.execute("DELETE FROM new_urls")
        # WHERE data_provider IS NOT NULL
        self.db_connection.connection.commit()
        
        insert_sql = "INSERT INTO new_urls (data_provider, ticker, historical_data_url, live_data_url) VALUES (%s,%s,%s,%s)"
        self.db_connection.cursor.executemany(insert_sql, self.data_set[:])
        self.db_connection.connection.commit()

        # get_number_of_delisted_assets_sql = f"""
        #     select count(*) from assets
        #     left join new_urls on new_urls.data_provider = assets.data_provider and new_urls.ticker = assets.ticker
        #     where assets.data_provider = '{self.data_provider}' and new_urls.data_provider is null                
        # """
        # self.db_connection.cursor.execute(get_number_of_delisted_assets_sql)
        # number_of_delisted_assets = self.db_connection.cursor.fetchall()

        # check if the assets table has records that are not in the temp table
        remove_delisted_assets_sql = f"""
            DELETE assets from assets
            left join new_urls on new_urls.data_provider = assets.data_provider and new_urls.ticker = assets.ticker
            where assets.data_provider = '{self.data_provider}' and new_urls.data_provider is null                
        """
        self.db_connection.cursor.execute(remove_delisted_assets_sql)
        self.db_connection.connection.commit()

        # get_number_of_newly_listed_assets_sql = f"""
        #     select count(*) from new_urls
        #     left join binance_assets on new_urls.ticker = binance_assets.ticker
        #     where binance_assets.live_data_url <> new_urls.live_data_url 
        #         or binance_assets.id is null;
        # """
        # self.db_connection.cursor.execute(get_number_of_newly_listed_assets_sql)
        # number_of_newly_listed_assets = self.db_connection.cursor.fetchall()

        add_newly_listed_assets_sql = f"""
            insert into assets
                select new_urls.* from new_urls
                left join assets on new_urls.data_provider = assets.data_provider and new_urls.ticker = assets.ticker
                where new_urls.data_provider = '{self.data_provider}' and assets.ticker is null;
        """
        self.db_connection.cursor.execute(add_newly_listed_assets_sql)
        self.db_connection.connection.commit()

        print(f'Inserted all assets from {self.data_provider}')

    def create_ID_column(self):
        create_id_column_sql = f"""ALTER TABLE `DummyData`.`assets` 
            ADD COLUMN `id` INT NOT NULL AUTO_INCREMENT AFTER `live_data_req_body`,
            ADD UNIQUE INDEX `id_UNIQUE` (`id` ASC) VISIBLE;
        ;"""
        self.db_connection.cursor.execute(create_id_column_sql)
        self.db_connection.connection.commit()
