import asyncio
import find_parent
from OHLC_table_updater.OHLC_db import OHLC_DB
from OHLC_table_updater.PriceData.Get_OHLC_Class_Async import Import_OHLC_Data_Async

class update_OHLC_table:
    def __init__(self):
        # # Get all listed Asset URL's from DB
        self.ohlc_tables = OHLC_DB()
        self.all_data_providers = self.ohlc_tables.fetch_distinct_data_providers()
        
        self.all_configs = []
        self.all_asset_dicts = []

        
        # dicts = self.ohlc_tables.return_all_asset_dicts_from_dataprovider('Kraken')
        # self.all_asset_dicts.append({
        #     'data_provider': 'Kraken',
        #     'asset_dicts': dicts
        # })

        # dicts = self.ohlc_tables.return_all_asset_dicts_from_dataprovider('Binance')
        # self.all_asset_dicts.append({
        #     'data_provider': 'Binance',
        #     'asset_dicts': dicts
        # })


        # dicts = self.ohlc_tables.return_all_asset_dicts_from_dataprovider('Alpaca')
        # self.all_asset_dicts.append({
        #     'data_provider': 'Alpaca',
        #     'asset_dicts': dicts
        # })

        for provider in self.all_data_providers:
            dicts = self.ohlc_tables.fetch_assets_by_provider(provider)
            self.all_asset_dicts.append({
                'data_provider': provider,
                'asset_dicts': dicts
            })

        config = {
            'Candle_Size': '1_Day',
            'Start_Time': None,
            'End_Time': None,
            'Data_Set_Size': None
        }

        self.all_configs.append(config)

    def fetch_OHLC_from_API_and_update_table(self):
        # Get PriceData
        for configuration in self.all_configs:
            Pricedata = Import_OHLC_Data_Async(configuration, self.all_asset_dicts)
            asset_dicts_with_ohlc = Pricedata.OHLC_Price_List_for_DB()

            async def insert(asset,index):
                if len(asset['OHLC'])>0:
                # Get First and last date of the dataset
                    self.ohlc_tables.insert_first_and_last_date_into_assets_table(
                        asset,
                        asset['OHLC'][0][0],
                        asset['OHLC'][-1][0]
                    )
                    self.ohlc_tables.insert_into_temp_table(asset['OHLC'])
                    # Join on TimeStamp
                    self.ohlc_tables.insert_into_OHLC_table()
                    print('Inserted',asset['data_provider'],asset['ticker'],index)
                else:
                    # delete the asset from the assets Table
                    self.ohlc_tables.delete_row_by_data_provider_and_ticker(
                        asset
                    )
                    print('Could not fetch, deleted: ',asset['data_provider'],asset['ticker'],index)
            
            async def main():
                tasks = [insert(asset, index) for index,asset in enumerate(asset_dicts_with_ohlc)]
                await asyncio.gather(*tasks)

            asyncio.run(main())
