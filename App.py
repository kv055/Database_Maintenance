# sketch of the final execusion script

from Assets_table_updater.update_asset_table_class import update_asset_table
from OHLC_table_updater.Async_update_OHLC import update_OHLC_table

asset_table = update_asset_table()
asset_table.set_Alpaca_URL_List()
asset_table.enter_into_db()
asset_table.set_Binance_URL_List()
asset_table.enter_into_db()
asset_table.set_Kraken_URL_List()
asset_table.enter_into_db()
asset_table.create_ID_column()

ohlc_table = update_OHLC_table()
ohlc_table.fetch_OHLC_from_API_and_update_table()

l = 0
# ToDo: Instead of just logging with Print either 
# write everything in a txt file or finde more sophisticated 
# start and stop time at the execution of thiis file and log it