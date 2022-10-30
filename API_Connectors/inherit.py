import alpaca_trade_api.rest as tradeapi
from alpaca_trade_api.entity import Asset
from typing import List

Assets = List[Asset]

class Alpaca_Inherited(tradeapi):
    def __init__(self) -> None:
        super().__init__()

    def list_tradable_assets(self) -> Assets:
        """Get a list of assets"""
        params = {
            'status': 'active',
            'tradable': 'true'
        }
        resp = self.get('/assets', params)
        if self._use_raw_data:
            return resp
        else:
            return [self.response_wrapper(o, Asset) for o in resp]