import asyncio
import find_parent
from OHLC_table_updater.OHLC_db import OHLC_DB
from OHLC_table_updater.PriceData.Get_OHLC_Class_Async import Import_OHLC_Data_Async

class update_OHLC_table:
    def __init__(self):
        pass

    def return_OHLC_from_API(self, all_asset_dicts):
        price_data_list = []
        # Get PriceData
        for configuration in all_asset_dicts:
            ohlc = Import_OHLC_Data_Async(configuration, all_asset_dicts)
            price_data_list.append(ohlc.OHLC_Price_List_for_DB())

        return price_data_list