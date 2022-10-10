import os
from dotenv import load_dotenv
from datetime import datetime
import alpaca_trade_api as tradeapi

class Alpaca:
    def __init__(self):
        load_dotenv()
        pub_key = os.getenv('ALPACA_PAPERTRADING_PUB_KEY')
        priv_key = os.getenv('ALPACA_PAPERTRADING_PRIV_KEY')
        alpaca_url = os.getenv('ALPACA_PAPERTRADING_URL')

        # pub_key = os.getenv('ALPACA_PUB_KEY')
        # priv_key = os.getenv('ALPACA_PRIV_KEY')
        # alpaca_url = os.getenv('ALPACA_URL')
        self.instance = tradeapi.REST(
            pub_key , 
            priv_key,
            alpaca_url
        )

    def get_assets(self):
        return self.instance.list_assets()
        
    def get_OHLC(self,asset, timeframe):
        
        Barset = self.instance.get_bars(
            asset,
            timeframe, 
            "1980-01-01", 
            "2022-10-08"
        )

        return Barset._raw


        
    