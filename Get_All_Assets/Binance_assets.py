import requests

def all_pairs_Binance():
    request_all_Binance_pairs = requests.get('https://api.binance.com/api/v3/exchangeInfo')
    Binance_pairs_raw = request_all_Binance_pairs.json()

    Binance_pairs = []
    # names = []

    for pair in Binance_pairs_raw['symbols']:
        Binance_pairs.append(pair['symbol'])
    Binance_pairs.sort()
    return Binance_pairs

def all_links_Binance():
    pairs = all_pairs_Binance()
    exchange_name = 'Binance'
    live_data_URIs = []
    ohlc_data_URIs = []
    for pair in pairs:
        ohlc_data_URI = 'https://api.binance.com/api/v3/klines?symbol='+pair+'&interval='
        ohlc_data_URIs.append(ohlc_data_URI)
        live_data_URI = 'https://api.binance.com/api/v3/ticker/price?symbol='+pair
        live_data_URIs.append(live_data_URI)
    
    # return tuples so that it can be inserted
    db_format_content = []
    if len(pairs) == len(ohlc_data_URIs) == len(live_data_URIs):
        for i in range(0, len(pairs)):
            db_format_content.append(
                (pairs[i],exchange_name,ohlc_data_URIs[i],live_data_URIs[i])
            )
            
    return db_format_content