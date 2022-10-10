import find_parent

from OHLC_table_updater.PriceData.Get_OHLC_Class import Import_OHLC_Data
from OHLC_table_updater.OHLC_db import OHLC_DB

class update_OHLC_table:
    def __init__(self):
        # Get all listed Asset URL's from DB
        self.ohlc_tables = OHLC_DB()
        self.all_asset_URLs = self.ohlc_tables.return_all_asset_URLs_to_fetch_OHLC()
        self.all_configs = []
        self.timeframe = '1_Day'
        config = {
            'Candle_Size': self.timeframe,
            'Start_Time': None,
            'End_Time': None,
            'Data_Set_Size': None
        }
        self.all_configs.append(config)

    def fetch_OHLC_from_API_and_update_table(self):
        # Get PriceData
        for configuration in self.all_configs:
            Pricedata = Import_OHLC_Data(configuration)
            for asset in self.all_asset_URLs:
                # Fetch the Price Data Sets from external API's
                Formated_OHLC_Data_Set = Pricedata.OHLC_Price_List_for_DB(asset, self.timeframe)
                self.ohlc_tables.insert_into_temp_table(Formated_OHLC_Data_Set)
                # Join on TimeStamp
                self.ohlc_tables.insert_into_OHLC_table()
                print(asset['ticker'],asset['data_provider'])


