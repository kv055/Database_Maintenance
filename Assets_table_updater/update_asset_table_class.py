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
        self.db_name = 'Financial_Data'
        self.db_connection = SQL_Server(self.db_name)
        # Create Assets Table if it doesnts exist
        create_assets_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {self.db_name}.`Assets` (
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

        # Delete id column if table exists
        delete_id_column_sql = f"""
            SET @column_to_drop = 'id';
            SET @index_to_drop = 'id_UNIQUE';

            IF EXISTS (
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = '{self.db_name}' AND TABLE_NAME = 'Assets' AND COLUMN_NAME = @column_to_drop
            ) THEN
                ALTER TABLE {self.db_name}.`Assets`
                DROP COLUMN `id`;
            END IF;

            IF EXISTS (
                SELECT INDEX_NAME
                FROM INFORMATION_SCHEMA.STATISTICS
                WHERE TABLE_SCHEMA = '{self.db_name}' AND TABLE_NAME = 'Assets' AND INDEX_NAME = @index_to_drop
            ) THEN
                ALTER TABLE {self.db_name}.`Assets`
                DROP INDEX `id_UNIQUE`;
            END IF;
        """

        self.db_connection.cursor.execute(delete_id_column_sql, multi=True)
        self.db_connection.connection.commit()

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
      
        # create temp table to hold all new data 
        self.db_connection.cursor.execute("CREATE TEMPORARY TABLE IF NOT EXISTS newly_fetched_assets LIKE Assets")
        self.db_connection.connection.commit()
        
        # no clue what that does
        self.db_connection.cursor.execute("DELETE FROM newly_fetched_assets")
        self.db_connection.connection.commit()
        
        insert_sql = "INSERT INTO newly_fetched_assets (data_provider, ticker, historical_data_url, live_data_url) VALUES (%s,%s,%s,%s)"
        self.db_connection.cursor.executemany(insert_sql, self.data_set[:])
        self.db_connection.connection.commit()

        # get_number_of_delisted_assets_sql = f"""
        #     select count(*) from assets
        #     left join newly_fetched_assets on newly_fetched_assets.data_provider = assets.data_provider and newly_fetched_assets.ticker = assets.ticker
        #     where assets.data_provider = '{self.data_provider}' and newly_fetched_assets.data_provider is null                
        # """
        # self.db_connection.cursor.execute(get_number_of_delisted_assets_sql)
        # number_of_delisted_assets = self.db_connection.cursor.fetchall()

        # check if the assets table has records that are not in the temp table
        remove_delisted_assets_sql = f"""
            DELETE Assets from Assets
            left join newly_fetched_assets on newly_fetched_assets.data_provider = Assets.data_provider and newly_fetched_assets.ticker = Assets.ticker
            where Assets.data_provider = '{self.data_provider}' and newly_fetched_assets.data_provider is null                
        """
        self.db_connection.cursor.execute(remove_delisted_assets_sql)
        self.db_connection.connection.commit()

        # get_number_of_newly_listed_assets_sql = f"""
        #     select count(*) from newly_fetched_assets
        #     left join binance_assets on newly_fetched_assets.ticker = binance_assets.ticker
        #     where binance_assets.live_data_url <> newly_fetched_assets.live_data_url 
        #         or binance_assets.id is null;
        # """
        # self.db_connection.cursor.execute(get_number_of_newly_listed_assets_sql)
        # number_of_newly_listed_assets = self.db_connection.cursor.fetchall()

        add_newly_listed_assets_sql = f"""
            insert into {self.db_name}.Assets
                select newly_fetched_assets.* from newly_fetched_assets
                left join Assets on newly_fetched_assets.data_provider = Assets.data_provider and newly_fetched_assets.ticker = Assets.ticker
                where newly_fetched_assets.data_provider = '{self.data_provider}' and Assets.ticker is null;
        """
        self.db_connection.cursor.execute(add_newly_listed_assets_sql)
        self.db_connection.connection.commit()

        print(f'Inserted all assets from {self.data_provider}')

    def create_ID_column(self):
        create_id_column_sql = f"""ALTER TABLE {self.db_name}.`Assets` 
            ADD COLUMN `id` INT NOT NULL AUTO_INCREMENT AFTER `live_data_req_body`,
            ADD UNIQUE INDEX `id_UNIQUE` (`id` ASC) VISIBLE;
        ;"""
        self.db_connection.cursor.execute(create_id_column_sql)
        self.db_connection.connection.commit()
