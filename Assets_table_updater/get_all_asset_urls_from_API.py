import requests
# import os
# from dotenv import load_dotenv

import find_parent
from API_Connectors.AlpacaConnector import Alpaca

class all_listed_assets:
    def __init__(self):
        # Fetch all listed Assets from the Alpaca API
        # load_dotenv()
        # pub = os.getenv('ALPACA_PAPERTRADING_PUB_KEY')
        # priv = os.getenv('ALPACA_PAPERTRADING_PRIV_KEY')
        # api_url = os.getenv('ALPACA_PAPERTRADING_URL')
        paper_trading_connector = Alpaca(
            # pub,
            # priv,
            # api_url
        )
        self.Alpaca_assets_raw = paper_trading_connector.get_assets()
        
        # Fetch all listed Assets from the Binance API
        request_all_Binance_pairs = requests.get('https://api.binance.com/api/v3/exchangeInfo')
        self.Binance_pairs_raw = request_all_Binance_pairs.json()
        
        # Fetch all listed Assets from the Kraken API
        request_all_Kraken_pairs = requests.get('https://api.kraken.com/0/public/AssetPairs')
        self.Kraken_pairs_raw = request_all_Kraken_pairs.json()

    def all_links_Kraken(self):
        Kraken_pairs = []

        for pair in self.Kraken_pairs_raw['result']:
            Kraken_pairs.append(pair)
        
        live_data_URIs = []
        ohlc_data_URIs = []
        for pair in Kraken_pairs:
            ohlc_data_URI = 'https://api.kraken.com/0/public/OHLC?pair='+pair+'&interval='
            ohlc_data_URIs.append(ohlc_data_URI)
            live_data_URI = 'https://api.kraken.com/0/public/Ticker?pair='+pair
            live_data_URIs.append(live_data_URI)

        # return tuples so that it can be inserted
        db_format_content = []
        if len(Kraken_pairs) == len(ohlc_data_URIs) == len(live_data_URIs):
            for index,asset_name in enumerate(Kraken_pairs):
                db_format_content.append(
                    ("Kraken", asset_name, ohlc_data_URIs[index], live_data_URIs[index])
                )

        return db_format_content

    def all_links_Binance(self):
        Binance_pairs = []

        for pair in self.Binance_pairs_raw['symbols']:
            Binance_pairs.append(pair['symbol'])
        Binance_pairs.sort()

        live_data_URIs = []
        ohlc_data_URIs = []
        for pair in Binance_pairs:
            ohlc_data_URI = 'https://api.binance.com/api/v3/klines?symbol='+pair+'&interval='
            ohlc_data_URIs.append(ohlc_data_URI)
            live_data_URI = 'https://api.binance.com/api/v3/ticker/price?symbol='+pair
            live_data_URIs.append(live_data_URI)
        
        # return tuples so that it can be inserted
        db_format_content = []
        if len(Binance_pairs) == len(ohlc_data_URIs) == len(live_data_URIs):
            for index,asset_name in enumerate(Binance_pairs):
                db_format_content.append(
                    ('Binance',asset_name,ohlc_data_URIs[index],live_data_URIs[index])
                )
                
        return db_format_content

    def all_links_Alpaca(self):
        alpaca_assets = [asset for asset in self.Alpaca_assets_raw if asset['tradable'] == True]

        Alpaca_tickers = []

        for asset in alpaca_assets:
            Alpaca_tickers.append(asset['symbol'])

        live_data_URIs = []
        ohlc_data_URIs = []

        for symbol in Alpaca_tickers:
            ohlc_data_URI = '/stocks/'+symbol+'/bars'
            ohlc_data_URIs.append(ohlc_data_URI)
            live_data_URI = '/stocks/'+symbol+'/bars/latest'
            live_data_URIs.append(live_data_URI)

        # return tuples so that it can be inserted
        db_format_content = []
        if len(Alpaca_tickers) == len(ohlc_data_URIs) == len(live_data_URIs):
            for index,asset_name in enumerate(Alpaca_tickers):
                db_format_content.append(
                    ('Alpaca',asset_name,ohlc_data_URIs[index],live_data_URIs[index])
                )
                
        return db_format_content

# test = all_listed_assets()
# Al =test.all_links_Alpaca()
# Bi =test.all_links_Binance()
# Kr =test.all_links_Kraken()
# l=0