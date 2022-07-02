

import os
from dotenv import load_dotenv
from API_Connectors.AlpacaConnector import Alpaca


# get all stocks
def all_tickers_Alpaca():
    load_dotenv()
    # get .env variables for Alpaca Authenticattion
    pub = os.getenv('ALPACA_PAPERTRADING_PUB_KEY')
    priv = os.getenv('ALPACA_PAPERTRADING_PRIV_KEY')
    api_url = os.getenv('ALPACA_PAPERTRADING_URL')

    paper_trading_connector = Alpaca(
        pub,
        priv,
        api_url
    )


    all_assets = paper_trading_connector.get_assets()

    alpaca_assets = [asset for asset in all_assets if asset['tradable'] == True]

    Alpaca_tickers = []

    for asset in alpaca_assets:
        Alpaca_tickers.append(asset['symbol'])

    return Alpaca_tickers

def all_links_Alpaca():
    tickers = all_tickers_Alpaca()
    data_provider_name = 'Alpaca'

    live_data_URIs = []
    ohlc_data_URIs = []

    for symbol in tickers:
        ohlc_data_URI = '/stocks/'+symbol+'/bars'
        ohlc_data_URIs.append(ohlc_data_URI)
        live_data_URI = '/stocks/'+symbol+'/bars/latest'
        live_data_URIs.append(live_data_URI)

    # return tuples so that it can be inserted
    db_format_content = []
    if len(tickers) == len(ohlc_data_URIs) == len(live_data_URIs):
        for i in range(0, len(tickers)):
            db_format_content.append(
                (tickers[i],data_provider_name,ohlc_data_URIs[i],live_data_URIs[i])
            )
            
    return db_format_content
