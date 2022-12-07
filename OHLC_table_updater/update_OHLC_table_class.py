import find_parent
from OHLC_table_updater.OHLC_db import OHLC_DB
from OHLC_table_updater.PriceData.Get_OHLC_Class import Import_OHLC_Data


class update_OHLC_table:
    def __init__(self):
        # Get all listed Asset URL's from DB
        self.ohlc_tables = OHLC_DB()
        self.all_asset_dicts = self.ohlc_tables.return_all_asset_dicts()
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
            for index,asset in enumerate(self.all_asset_dicts):
                # Fetch the Price Data Sets from external API's
                Formated_OHLC_Data_Set = Pricedata.OHLC_Price_List_for_DB(asset, self.timeframe)
                if len(Formated_OHLC_Data_Set)>0:
                    # Get First and last date of the dataset
                    self.ohlc_tables.insert_first_and_last_date_into_assets_table(
                        asset,
                        Formated_OHLC_Data_Set[0][0],
                        Formated_OHLC_Data_Set[-1][0]
                    )
                    self.ohlc_tables.insert_into_temp_table(Formated_OHLC_Data_Set)
                    # Join on TimeStamp
                    self.ohlc_tables.insert_into_OHLC_table()
                    print('Inserted',asset['data_provider'],asset['ticker'],index)
                else:
                    # delete the asset from the assets Table
                    self.ohlc_tables.delete_row_by_data_provider_and_ticker(
                        asset
                    )
                    print('could not fetch for now deleted selected asset: ',asset['data_provider'],asset['ticker'])

