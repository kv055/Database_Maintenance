import find_parent
from API_Connectors.aws_sql_connect import SQL_Server

class OHLC_DB:
    def __init__(self):
        self.Assets_db = SQL_Server('DummyData')
        self.OHLC_db = SQL_Server('OHLC')
        self.Time_Frame = '1_Day'

    def return_all_asset_URLs_to_fetch_OHLC(self):
        
        # querry all assets from DB
        urls_to_fetch = []
        query =  f"""
            SELECT * from DummyData.assets
            WHERE data_provider = 'Kraken'
        """
        self.Assets_db.cursor.execute(query)
        table = self.Assets_db.cursor.fetchall()
        self.Assets_db.close()

        # formate and return the data
        for row in table:
            urls_to_fetch.append({
                'Data_Provider': row[0],
                'Ticker': row[1],
                'URL':row[2],
            })
        return urls_to_fetch

    def create_all_OHLC_tables(self):
        create_OHLC_table = f"""
            CREATE TABLE IF NOT EXISTS {self.Time_Frame} (
                `Date` DATETIME NOT NULL,
                `Open` DOUBLE NOT NULL,
                `High` DOUBLE NOT NULL,
                `Low` DOUBLE NOT NULL,
                `Close` DOUBLE NOT NULL,
                `Data_Provider` varchar(45) NOT NULL,
                `Ticker` varchar(45) NOT NULL,
                PRIMARY KEY (`Data_Provider`,`Ticker`,`Date`)
        )
        """
        self.OHLC_db.cursor.execute(create_OHLC_table)
        self.OHLC_db.connection.commit()

        create_Temp_table_sql = f"""
            CREATE TABLE IF NOT EXISTS new_ohlc_temp_table LIKE {self.Time_Frame}
        """
        self.OHLC_db.cursor.execute(create_Temp_table_sql)
        self.OHLC_db.connection.commit()

    def insert_all_rows_into_temp_table(self,Formated_OHLC_Data_Set):
        self.OHLC_db.cursor.execute("DELETE FROM new_ohlc_temp_table")
        self.OHLC_db.connection.commit()
        # insert it into the temp rable
        insert_sql = "INSERT INTO new_ohlc_temp_table (Date, Open, High, Low, Close, Data_Provier, Ticker) VALUES (%s,%s,%s,%s,%s,%s,%s)"
        self.OHLC_db.cursor.executemany(insert_sql, Formated_OHLC_Data_Set)
        self.OHLC_db.connection.commit()

    def insert_new_rows_into_OHLC_table(self):
        join_sql = f"""
            insert into 1_Day
                select new_ohlc_temp_table.* from new_ohlc_temp_table
                left join 1_Day on new_ohlc_temp_table.Date = 1_Day.Date
                -- where 1_Day.Date is null
        """
        self.OHLC_db.cursor.execute(join_sql)
        self.OHLC_db.connection.commit()
