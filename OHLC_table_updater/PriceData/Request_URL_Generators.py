
class generate_request:
    def __init__(self, ohlc_config):
        self.candle_size = ohlc_config['Candle_Size']

    def Alpaca(self, asset_url):
        url = 0
        request_body = 0
        return [url,request_body]

    def Binance(self, asset_url):
        # Interval
        if self.candle_size == '1_Min':
            interval = '1m'
        if self.candle_size == '3_Min':
            interval = '3m'
        if self.candle_size == '5_Min':
            interval = '5m'
        if self.candle_size == '15_Min':
            interval = '15m'
        if self.candle_size == '30_Min':
            interval = '30m'
        if self.candle_size == '1_Hour':
            interval = '1h'
        if self.candle_size == '2_Hour':
            interval = '2h'
        if self.candle_size == '4_Hour':
            interval = '4h'
        if self.candle_size == '6_Hour':
            interval = '6h'
        if self.candle_size == '8_Hour':
            interval = '8h'
        if self.candle_size == '12_Hour':
            interval = '12h'
        if self.candle_size == '1_Day':
            interval = '1d'
        if self.candle_size == '3_Day':
            interval = '3d'
        if self.candle_size == '1_Week':
            interval = '1w'
        if self.candle_size == '1_Month':
            interval = '1M'

        url_to_fetch = f'{asset_url}{interval}'        
        return url_to_fetch

    def Kraken(self, asset_url):
        # Interval
        if self.candle_size == '1_Min':
            interval = '1'
        # if self.candle_size == '3_Min':
        #     interval = '3m'
        if self.candle_size == '5_Min':
            interval = '5'
        if self.candle_size == '15_Min':
            interval = '15'
        if self.candle_size == '30_Min':
            interval = '30'
        if self.candle_size == '1_Hour':
            interval = '60'
        # if self.candle_size == '2_Hour':
        #     interval = '2h'
        if self.candle_size == '4_Hour':
            interval = '240'
        # if self.candle_size == '6_Hour':
        #     interval = '6h'
        # if self.candle_size == '8_Hour':
        #     interval = '8h'
        # if self.candle_size == '12_Hour':
        #     interval = '12h'
        if self.candle_size == '1_Day':
            interval = '1440'
        # if self.candle_size == '3_Day':
        #     interval = '3d'
        if self.candle_size == '1_Week':
            interval = '10080'
        if self.candle_size == '2_Week':
            interval = '21600'
        # if self.candle_size == '1_Month':
        #     interval = '21600'

        url_to_fetch = f'{asset_url}{interval}'        
        return url_to_fetch