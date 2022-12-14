import asyncio
import aiohttp

import find_parent
from OHLC_table_updater.OHLC_db import OHLC_DB
# from OHLC_table_updater.PriceData.Get_OHLC_Class import Import_OHLC_Data
from OHLC_table_updater.PriceData.Get_OHLC_Class_Async import Import_OHLC_Data_Async

class update_OHLC_table:
    def __init__(self):
        # # Get all listed Asset URL's from DB
        self.ohlc_tables = OHLC_DB()
        # self.all_data_providers = self.ohlc_tables.return_all_data_providers()
        
        self.all_asset_dicts = [
            # {
            #     'data_provider': 'Binance', 
            #     'asset_dicts': [{'data_provider': 'Binance', 'ticker': '1INCHBTC', 'historical_data_url': 'https://api.binance.com/api/v3/klines?symbol=1INCHBTC&interval=', 'historical_data_req_body': None, 'live_data_url': 'https://api.binance.com/api/v3/ticker/price?symbol=1INCHBTC', 'live_data_req_body': None, 'id': 10843, 'first_available_datapoint': None, 'last_available_datapoint': None}]
            # }
        ]
        # for provider in self.all_data_providers:
        #     dicts = self.ohlc_tables.return_all_asset_dicts_from_dataprovider(provider['data_provider'])
        #     self.all_asset_dicts.append({
        #         'data_provider': provider['data_provider'],
        #         'asset_dicts': dicts
        #     })

        dicts = self.ohlc_tables.return_all_asset_dicts_from_dataprovider('Kraken')
        self.all_asset_dicts.append({
            'data_provider': 'Kraken',
            'asset_dicts': dicts
        })

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
            Pricedata = Import_OHLC_Data_Async(configuration, self.all_asset_dicts)
            all_fomated_ohlc_datasets = Pricedata.OHLC_Price_List_for_DB()

            for index,Formated_OHLC_Data_Set in enumerate(all_fomated_ohlc_datasets):
                if len(Formated_OHLC_Data_Set)>0:
                    pass
            #         # Get First and last date of the dataset
            #         self.ohlc_tables.insert_first_and_last_date_into_assets_table(
            #             asset,
            #             Formated_OHLC_Data_Set[0][0],
            #             Formated_OHLC_Data_Set[-1][0]
            #         )
            #         self.ohlc_tables.insert_into_temp_table(Formated_OHLC_Data_Set)
            #         # Join on TimeStamp
            #         self.ohlc_tables.insert_into_OHLC_table()
            #         print('Inserted',asset['data_provider'],asset['ticker'],index)
            #     else:
            #         # delete the asset from the assets Table
            #         self.ohlc_tables.delete_row_by_data_provider_and_ticker(
            #             asset
            #         )
            #         print('could not fetch for now deleted selected asset: ',asset['data_provider'],asset['ticker'])

hure = update_OHLC_table()
hure.fetch_OHLC_from_API_and_update_table()