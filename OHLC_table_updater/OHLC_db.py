# import find_parent
# from API_Connectors.aws_sql_connect import SQL_Server

# # initial setup, only get executed when no tables exist inthe OHLC db,
# # but when the tables alredy exist then it gets skipped
# # executed inside the init of  update_OHLC_table_class

# class OHLC_DB:
#     def __init__(self):
#         self.db_name = 'Financial_Data'
#         self.db_connection = SQL_Server(self.db_name)
#         self.create_all_OHLC_tables()

#     def return_all_data_providers(self):
#         query_all_data_providers_sql =  f"""
#             SELECT DISTINCT data_provider from {self.db_name}.Assets
#         """
#         self.db_connection.cursor.execute(query_all_data_providers_sql)
#         table = self.db_connection.cursor.fetchall()
#         return table

#     def return_all_asset_dicts_from_dataprovider(self,dataprovider):
#         # querry all assets from DB
#         query =  f"""
#             SELECT * from {self.db_name}.Assets
#             WHERE data_provider = '{dataprovider}'
#         """
#         self.db_connection.cursor.execute(query)
#         table = self.db_connection.cursor.fetchall()
#         return table

#     def return_all_asset_dicts(self):
#         # querry all assets from DB
#         query =  f"""
#             SELECT * from {self.db_name}.Assets
#         """
#         self.db_connection.cursor.execute(query)
#         table = self.db_connection.cursor.fetchall()
#         return table

#     def create_all_OHLC_tables(self):
#         create_OHLC_table = f"""
#             CREATE TABLE IF NOT EXISTS OHLC (
#                 `Date` DATETIME NOT NULL,
#                 `Open` DOUBLE NOT NULL,
#                 `High` DOUBLE NOT NULL,
#                 `Low` DOUBLE NOT NULL,
#                 `Close` DOUBLE NOT NULL,
#                 `Average` DOUBLE NOT NULL,
#                 `Data_Provider` varchar(45) NOT NULL,
#                 `Ticker` varchar(45) NOT NULL,
#                 `Time_Frame` varchar(45) NOT NULL,
#                 PRIMARY KEY (`Data_Provider`,`Ticker`,`Date`)
#         )
#         """
#         self.db_connection.cursor.execute(create_OHLC_table)
#         self.db_connection.connection.commit()

#         # create_Temp_table_sql = f"""
#         #     CREATE TABLE IF NOT EXISTS temporary_new_OHLC LIKE OHLC
#         # """
#         # self.db_connection.cursor.execute(create_Temp_table_sql)
#         # self.db_connection.connection.commit()

#         create_Temp_table_sql = f"""
#             CREATE TEMPORARY TABLE temporary_new_OHLC LIKE OHLC
#         """
#         self.db_connection.cursor.execute(create_Temp_table_sql)
#         self.db_connection.connection.commit()

#     def insert_first_and_last_date_into_assets_table(self, asset_dict, first_date, last_date):
#         update_first_and_last_dates_sql = f"""
#             UPDATE {self.db_name}.Assets 
#             SET first_available_datapoint = {first_date}, 
#                 last_available_datapoint = {last_date} 
#             WHERE data_provider = '{asset_dict['data_provider']}' and ticker = '{asset_dict['ticker']}';
#         """
#         self.db_connection.cursor.execute(update_first_and_last_dates_sql)
#         self.db_connection.connection.commit()

#     def delete_row_by_data_provider_and_ticker(self, asset_dict):
#         delete_row_by_provider_and_ticker_sql = f"""
#             DELETE FROM {self.db_name}.Assets 
#             WHERE data_provider = '{asset_dict['data_provider']}' and ticker = '{asset_dict['ticker']}';
#         """
#         self.db_connection.cursor.execute(delete_row_by_provider_and_ticker_sql)
#         self.db_connection.connection.commit()

#     def insert_into_temp_table(self,OHLC_Data_Set):
#         # # delete old Data_set
#         # self.db_connection.cursor.execute("DELETE FROM temporary_new_OHLC")
#         # self.db_connection.connection.commit()
#         # # insert it into the temp rable
#         # insert_sql = f"INSERT INTO temporary_new_OHLC (Date, Open, High, Low, Close, Ticker, Data_Provider, Time_Frame) VALUES (FROM_UNIXTIME(%s),%s,%s,%s,%s,%s,%s,%s)"
#         # self.db_connection.cursor.executemany(insert_sql, OHLC_Data_Set)
#         # self.db_connection.connection.commit()
        
#         # delete old Data_set
#         self.db_connection.cursor.execute("DELETE FROM temporary_new_OHLC")
#         self.db_connection.connection.commit()
#         # insert it into the temp rable
#         insert_sql = f"INSERT INTO temporary_new_OHLC (Date, Open, High, Low, Close, Average, Ticker, Data_Provider, Time_Frame) VALUES (FROM_UNIXTIME(%s),%s,%s,%s,%s,%s,%s,%s,%s)"
#         self.db_connection.cursor.executemany(insert_sql, OHLC_Data_Set)
#         self.db_connection.connection.commit()

#     def insert_into_OHLC_table(self):
#         # Compare entries with left/right joins
#         # count_sql = f"""
#         #     select * from temporary_new_OHLC
#         #     left join OHLC 
#         #         ON temporary_new_self.OHLC_Table.Date = self.OHLC_Table.Date
#         #         AND temporary_new_self.OHLC_Table.Data_Provider = self.OHLC_Table.Data_Provider
#         #         AND temporary_new_self.OHLC_Table.Ticker = self.OHLC_Table.Ticker
#         #     where self.OHLC_Table.Date is null
#         # """
#         # self.db_connection.cursor.execute(count_sql)
#         # new_rows = self.db_connection.cursor.fetchall()

#         join_sql = f"""
#             insert into OHLC
#                 select temporary_new_self.OHLC_Table.* from temporary_new_OHLC
#                     left join OHLC 
#                         on temporary_new_self.OHLC_Table.Date = self.OHLC_Table.Date
#                         AND temporary_new_self.OHLC_Table.Data_Provider = self.OHLC_Table.Data_Provider
#                         AND temporary_new_self.OHLC_Table.Ticker = self.OHLC_Table.Ticker
#                     where self.OHLC_Table.Date is null
#         """
#         self.db_connection.cursor.execute(join_sql)
#         self.db_connection.connection.commit()



from sqlalchemy.sql import delete, insert, update, select, null, distinct

from ORM_Models.ORM_Models_Module import FinancialData


class OHLC_DB:
    def __init__(self):
        # establish connection to the Financiaal Data DB
        self.FinancialDataDb = FinancialData()
        self.session = self.FinancialDataDb.session
        self.engine = self.FinancialDataDb.engine
        self.metadata = self.FinancialDataDb.metadata
        self.Assets_Table = self.FinancialDataDb.return_Assets_Table()
        self.OHLC_Table = self.FinancialDataDb.return_OHLC_Table()
        self.create_OHLC_tables()


    def fetch_distinct_data_providers(self):
        # TO do fix this issue that only Kraken gets returned
        distinct_data_providers = (
            self.session
            .query(distinct(self.Assets_Table.c.data_provider))
            .all()
        )
        return [provider[0] for provider in distinct_data_providers]

    def fetch_assets_by_provider(self, dataprovider):
        
        assets = (
            self.session
            .query(self.Assets_Table)
            .filter(self.Assets_Table.c.data_provider == dataprovider)
            .all()
        )
        # Convert assets to a list of tuples
        # asset_tuples = [asset._to_tuple_instance() for asset in assets]
        return assets

    def fetch_all_assets(self):
        
        assets = self.session.query(self.Assets_Table).all()
        return assets

    def create_OHLC_tables(self):
        # Create OHLC Table
        self.OHLC_Table.create(self.engine, checkfirst=True)
        # create Temp Table

        # Reflect the schema of self.OHLC_Table
        self.metadata.reflect(bind=self.engine, schema='Financial_Data')
        table_structure = self.OHLC_Table.metadata.tables[self.OHLC_Table.name]

        # Specify the name for your temporary table
        temp_table_name = 'Financial_Data'

        # Create the temporary table using the reflected table structure
        self.temp_ohlc_table = table_structure.tometadata(self.metadata, schema=temp_table_name)

        # Create the temporary table in the database
        self.temp_ohlc_table.create(self.engine, checkfirst=True)

    


    def insert_first_and_last_date_into_assets_table(self, asset_dict, first_date, last_date):
        
        update_stmt = (
            update(self.Assets_Table)
            .where(
                (self.Assets_Table.c.data_provider == asset_dict['data_provider']) &
                (self.Assets_Table.c.ticker == asset_dict['ticker'])
            )
            .values(
                first_available_datapoint=first_date,
                last_available_datapoint=last_date
            )
        )
        self.session.execute(update_stmt)
        self.session.commit()

    def delete_row_by_data_provider_and_ticker(self, asset_dict):
        delete_stmt = (
            delete(self.Assets_Table)
            .where(
                (self.Assets_Table.c.data_provider == asset_dict['data_provider']) &
                (self.Assets_Table.c.ticker == asset_dict['ticker'])
            )
        )
        self.session.execute(delete_stmt)
        self.session.commit()

    def insert_into_temp_table(self, OHLC_Data_Set):
        self.session.query(self.temp_ohlc_table).delete()
        
        # # Delete all rows from the temp table (in case it was populated earlier)
        # delete_stmt = delete(self.temp_ohlc_table)
        # self.session.execute(delete_stmt)   
        # self.session.commit()
        print(OHLC_Data_Set[0][6])
        # Perform the bulk INSERT operation
        for data_tuple in OHLC_Data_Set:
            self.session.execute(
                self.temp_ohlc_table.insert().values(data_tuple)
            )
        self.session.commit()



    def insert_into_OHLC_table(self):
        # Define the select statement for the left join
        select_stmt = (
            select(self.OHLC_Table)
            .select_from(self.OHLC_Table)
            .join(
                self.OHLC_Table,
                onclause=(
                    self.OHLC_Table.Date == self.OHLC_Table.Date,
                    self.OHLC_Table.Data_Provider == self.OHLC_Table.Data_Provider,
                    self.OHLC_Table.Ticker == self.OHLC_Table.Ticker
                ),
                isouter=True
            )
            .where(self.OHLC_Table.Date.is_(None))
        )

        # Define the insert statement to insert rows into the OHLC table
        insert_stmt = insert(self.OHLC_Table).from_select(
            self.OHLC_Table.columns.keys(),
            select_stmt
        ).where(self.OHLC_Table.Date == null())

        # Execute the insert statement
        self.session.execute(insert_stmt)
        self.session.commit()
