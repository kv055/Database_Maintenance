import alpaca_trade_api as tradeapi

class Alpaca:
    def __init__(self, key, secr_key, select_api):
        self.alpaca_url = select_api
        self.pub_key = key
        self.priv_key = secr_key

        self.instance = tradeapi.REST(
            self.pub_key , 
            self.priv_key,
            self.alpaca_url
        )
    def get_assets(self):
        return self.instance.list_assets()
        
        
    