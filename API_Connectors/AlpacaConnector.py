import os
import asyncio
from datetime import datetime

import alpaca_trade_api as tradeapi
from dotenv import load_dotenv

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
        # def list_tradable_assets(self) -> Assets:
        # """Get a list of assets"""
        # params = {
        #     'status': 'active',
        #     'tradable': 'true'
        # }
        # resp = self.get('/assets', params)
        # if self._use_raw_data:
        #     return resp
        # else:
        #     return [self.response_wrapper(o, Asset) for o in resp]

        # all_tradable_assets = self.instance.list_tradable_assets()
        
        raw_assets = self.instance.list_assets()
        assets = [asset for asset in raw_assets if asset['status'] != 'inactive' and asset['tradable']]

        return assets
        
    async def get_OHLC(self,asset, timeframe):
        
        Barset = self.instance.get_bars(
            asset,
            timeframe, 
            "1980-01-01", 
            "2022-10-08"
        )

        return Barset._raw


        
    