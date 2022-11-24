import find_parent
from API_Connectors.aws_sql_connect import SQL_Server

# initial setup, only get executed when no tables exist inthe OHLC db,
# but when the tables alredy exist then it gets skipped
# executed inside the init of  update_OHLC_table_class

class OHLC_DB:
    def __init__(self):
        self.db_connection = SQL_Server('DummyData')
        self.create_all_OHLC_tables()

    def return_all_data_providers(self):
        query_all_data_providers_sql =  f"""
            SELECT DISTINCT data_provider from DummyData.assets
        """
        self.db_connection.cursor.execute(query_all_data_providers_sql)
        table = self.db_connection.cursor.fetchall()
        all_data_providers = []
        for pair in table:
            data_provider = list(pair.values())
            all_data_providers.append(data_provider[0])
        
        return all_data_providers

    def return_all_asset_URLs_from_dataprovider(self,dataprovider):
        # querry all assets from DB
        query =  f"""
            SELECT * from DummyData.assets
            WHERE assets.data_provider = '{dataprovider}'
        """
        self.db_connection.cursor.execute(query)
        table = self.db_connection.cursor.fetchall()
        return table

    def return_all_asset_dicts(self):
        # querry all assets from DB
        query =  f"""
            SELECT * from DummyData.assets
        """
        self.db_connection.cursor.execute(query)
        table = self.db_connection.cursor.fetchall()
        return table

    def create_all_OHLC_tables(self):
        create_OHLC_table = f"""
            CREATE TABLE IF NOT EXISTS OHLC (
                `Date` DATETIME NOT NULL,
                `Open` DOUBLE NOT NULL,
                `High` DOUBLE NOT NULL,
                `Low` DOUBLE NOT NULL,
                `Close` DOUBLE NOT NULL,
                `Average` DOUBLE NOT NULL,
                `Data_Provider` varchar(45) NOT NULL,
                `Ticker` varchar(45) NOT NULL,
                `Time_Frame` varchar(45) NOT NULL,
                PRIMARY KEY (`Data_Provider`,`Ticker`,`Date`)
        )
        """
        self.db_connection.cursor.execute(create_OHLC_table)
        self.db_connection.connection.commit()

        create_Temp_table_sql = f"""
            CREATE TABLE IF NOT EXISTS new_ohlc_temp_table LIKE OHLC
        """
        self.db_connection.cursor.execute(create_Temp_table_sql)
        self.db_connection.connection.commit()

        create_Temp_table_sql = f"""
            CREATE TEMPORARY TABLE temporary_new_OHLC LIKE OHLC
        """
        self.db_connection.cursor.execute(create_Temp_table_sql)
        self.db_connection.connection.commit()

    def insert_first_and_last_date_into_assets_table(self, first_date, last_date):
        update_first_and_last_dates_sql = f"""
            `first_available_datapoint`{first_date}
            `last_available_datapoint`{last_date}
        """
        self.db_connection.cursor.execute(update_first_and_last_dates_sql)
        self.db_connection.connection.commit()

    def insert_into_temp_table(self,OHLC_Data_Set):
        # # delete old Data_set
        # self.db_connection.cursor.execute("DELETE FROM temporary_new_OHLC")
        # self.db_connection.connection.commit()
        # # insert it into the temp rable
        # insert_sql = f"INSERT INTO temporary_new_OHLC (Date, Open, High, Low, Close, Ticker, Data_Provider, Time_Frame) VALUES (FROM_UNIXTIME(%s),%s,%s,%s,%s,%s,%s,%s)"
        # self.db_connection.cursor.executemany(insert_sql, OHLC_Data_Set)
        # self.db_connection.connection.commit()
        
        # delete old Data_set
        self.db_connection.cursor.execute("DELETE FROM new_ohlc_temp_table")
        self.db_connection.connection.commit()
        # insert it into the temp rable
        insert_sql = f"INSERT INTO new_ohlc_temp_table (Date, Open, High, Low, Close, Average, Ticker, Data_Provider, Time_Frame) VALUES (FROM_UNIXTIME(%s),%s,%s,%s,%s,%s,%s,%s,%s)"
        self.db_connection.cursor.executemany(insert_sql, OHLC_Data_Set)
        self.db_connection.connection.commit()

    def insert_into_OHLC_table(self):
        # Compare entries with left/right joins
        count_sql = f"""
            select * from new_ohlc_temp_table
            left join OHLC 
                ON new_ohlc_temp_table.Date = OHLC.Date
                AND new_ohlc_temp_table.Data_Provider = OHLC.Data_Provider
                AND new_ohlc_temp_table.Ticker = OHLC.Ticker
            where OHLC.Date is null
        """
        self.db_connection.cursor.execute(count_sql)
        new_rows = self.db_connection.cursor.fetchall()

        join_sql = f"""
            insert into OHLC
                select new_ohlc_temp_table.* from new_ohlc_temp_table
                    left join OHLC 
                        on new_ohlc_temp_table.Date = OHLC.Date
                        AND new_ohlc_temp_table.Data_Provider = OHLC.Data_Provider
                        AND new_ohlc_temp_table.Ticker = OHLC.Ticker
                    where OHLC.Date is null
        """
        self.db_connection.cursor.execute(join_sql)
        self.db_connection.connection.commit()

