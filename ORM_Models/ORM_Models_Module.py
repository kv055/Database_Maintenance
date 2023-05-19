from .sqalchemy_connect import SQL_Server
from sqlalchemy import Table, Column, Integer, Float, String
from sqlalchemy.types import DateTime

# class BackTesting:
#     def __init__(self) -> None:
#         self.db_name = 'Back_testing'
#         self.connection_instance = SQL_Server(self.db_name)


class FinancialData:
    def __init__(self) -> None:
        self.db_name = 'Financial_Data'
        self.connection_instance = SQL_Server(self.db_name)
        self.Base = self.connection_instance.get_base()
        self.session = self.connection_instance.get_session()
        self.metadata = self.connection_instance.get_metadata()
        self.engine = self.connection_instance.get_engine()

    def return_Assets_Table(self):
        self.assets_table = Table(
            'Assets',
            self.metadata,
            Column('data_provider', String(255), primary_key=True),
            Column('ticker', String(45), primary_key=True),
            Column('historical_data_url', String(255)),
            Column('historical_data_req_body', String(255)),
            Column('live_data_url', String(255)),
            Column('live_data_req_body', String(255)),
            Column('first_available_datapoint', DateTime, nullable=False),
            Column('last_available_datapoint', DateTime, nullable=False),
            extend_existing=True  # Add this line to extend the existing table
        )
        return self.assets_table

    def return_OHLC_Table(self):
        self.OHLC_table = Table(
            'OHLC',
            self.metadata,
            Column('Date', DateTime, primary_key=True),
            Column('Open', Float, nullable=False),
            Column('High', Float, nullable=False),
            Column('Low', Float, nullable=False),
            Column('Close', Float, nullable=False),
            Column('Average', Float, nullable=False),
            Column('Data_Provider', String(45), primary_key=True),
            Column('Ticker', String(45), primary_key=True),
            Column('Time_Frame', String(45), primary_key=True)
        )
        return self.OHLC_table
    
    def return_temp_OHLC_Table(self):
        self.temp_OHLC_Table = Table(
            'temp_OHLC',
            self.metadata,
            Column('Date', DateTime),
            Column('Open', Float, nullable=False),
            Column('High', Float, nullable=False),
            Column('Low', Float, nullable=False),
            Column('Close', Float, nullable=False),
            Column('Average', Float, nullable=False),
            Column('Data_Provider', String(45), primary_key=True),
            Column('Ticker', String(45), primary_key=True),
            Column('Time_Frame', String(45), primary_key=True),
            # Add the following line to make the table temporary
            prefixes=['TEMPORARY']
        )
        return self.temp_OHLC_Table
        

    # def return_OHLC_Table(self):
    #     self.OHLC_table = Table(
    #         'OHLC',
    #         self.metadata,
    #         Column('Date', DateTime),
    #         Column('Open', Float, nullable=False),
    #         Column('High', Float, nullable=False),
    #         Column('Low', Float, nullable=False),
    #         Column('Close', Float, nullable=False),
    #         Column('Average', Float, nullable=False),
    #         Column('Data_Provider', String(45), primary_key=True),
    #         Column('Ticker', String(45), primary_key=True),
    #         Column('Time_Frame', String(45), primary_key=True)
    #     )
    #     return self.OHLC_table
    
    # def return_temp_OHLC_Table(self):
    #     self.temp_OHLC_Table = Table(
    #         'temp_OHLC',
    #         self.metadata,
    #         Column('Date', DateTime),
    #         Column('Open', Float, nullable=False),
    #         Column('High', Float, nullable=False),
    #         Column('Low', Float, nullable=False),
    #         Column('Close', Float, nullable=False),
    #         Column('Average', Float, nullable=False),
    #         Column('Data_Provider', String(45), primary_key=True),
    #         Column('Ticker', String(45), primary_key=True),
    #         Column('Time_Frame', String(45), primary_key=True),
    #         # Add the following line to make the table temporary
    #         prefixes=['TEMPORARY']
    #     )
    #     return self.temp_OHLC_Table