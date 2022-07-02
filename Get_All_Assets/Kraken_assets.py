import requests

def all_pairs_Kraken():
    request_all_Kraken_pairs = requests.get('https://api.kraken.com/0/public/AssetPairs')
    Kraken_pairs_raw = request_all_Kraken_pairs.json()
    
    Kraken_pairs = []
    # names = []

    for pair in Kraken_pairs_raw['result']:
        Kraken_pairs.append(pair)
    return Kraken_pairs


def all_links_Kraken():
    pairs = all_pairs_Kraken()
    exchange_name = 'Kraken'
    live_data_URIs = []
    ohlc_data_URIs = []
    for pair in pairs:
        ohlc_data_URI = 'https://api.kraken.com/0/public/OHLC?pair='+pair+'&interval='
        ohlc_data_URIs.append(ohlc_data_URI)
        live_data_URI = 'https://api.kraken.com/0/public/Ticker?pair='+pair
        live_data_URIs.append(live_data_URI)

    # return tuples so that it can be inserted
    db_format_content = []
    if len(pairs) == len(ohlc_data_URIs) == len(live_data_URIs):
        for i in range(0, len(pairs)):
            db_format_content.append(
                (pairs[i],exchange_name,ohlc_data_URIs[i],live_data_URIs[i])
            )

    return db_format_content