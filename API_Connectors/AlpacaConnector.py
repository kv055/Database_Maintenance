import os
from dotenv import load_dotenv

import alpaca_trade_api as tradeapi

class Alpaca:
    def __init__(self):
        load_dotenv()
        pub_key = os.getenv('ALPACA_PAPERTRADING_PUB_KEY')
        priv_key = os.getenv('ALPACA_PAPERTRADING_PRIV_KEY')
        alpaca_url = os.getenv('ALPACA_PAPERTRADING_URL')
        self.instance = tradeapi.REST(
            pub_key , 
            priv_key,
            alpaca_url
        )

    def get_assets(self):
        return self.instance.list_assets()
        
    def get_OHLC(self,url, timeframe):
        # return
        test = self.instance.get_barset('AAPL','day')
        l = 0
    