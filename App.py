

from Assets_table_updater.update_asset_table_class import Get_all_assets
from OHLC_table_updater.Async_update_OHLC import update_OHLC_table

def Fetch_OHLC():
    Assets = Get_all_assets()
    # Alpaca_assets = Assets.set_Alpaca_URL_List()
    # Binance_assets = Assets.set_Binance_URL_List()
    Kraken_assets = Assets.set_Kraken_URL_List()

    ohlc_table = update_OHLC_table()
    return ohlc_table.return_OHLC_from_API([
        # {'data_provider':"Alpaca","Candle_Size":'1_Day','asset_dicts':Alpaca_assets},
        # {'data_provider':"Binance","Candle_Size":'1_Day','asset_dicts':Binance_assets},
        {'data_provider':"Kraken","Candle_Size":'1_Day','asset_dicts':Kraken_assets}
        ])


