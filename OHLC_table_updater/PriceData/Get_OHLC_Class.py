from datetime import datetime
import requests

import find_parent
from API_Connectors.AlpacaConnector import Alpaca
from OHLC_table_updater.PriceData.Request_URL_Generators import generate_request

# Sim_config = {
#     'Strategy':'dummy_data_strategy',
#     'Parameter':0,
#     'Candle_Size': '1_Day',
#     'Start_Time': None,
#     'End_Time': None,
#     'Data_Set_Size': None
# }
# asset_row = {
#     'historical_data_url': 'AAVEBUSD',
#     'Ticker': 'https://api.binance.com/api/v3/klines?symbol=AAVEBUSD&interval=',
#     'Data_Provider': 'Binance'
# }

# from Database_SQL.query_assets import return_all_asset_URLs
class Import_OHLC_Data:
    """Fetching OHLC Data from various api's"""
    def __init__(self, ohlc_config):
        self.fetch_config = ohlc_config
        self.request_args = generate_request(ohlc_config)
        # initialize Alpaca_Api connector instance
        self.Alpaca_API = Alpaca()

    def get_historical_OHLC(self, asset):
        if asset['data_provider'] == 'Alpaca':
            # Alpaca_to_fetch = self.request_args.Alpaca(asset['historical_data_url'])
            # url_to_fetch = Alpaca_to_fetch
            # req_body = Alpaca_to_fetch[1]
            ohlc_in_alpaca_raw_format = self.Alpaca_API.get_OHLC(asset['ticker'],'1Day')
            self.unformated_dataset = []
            for Bar_dict in ohlc_in_alpaca_raw_format:
                date_as_string = Bar_dict['t'].replace('Z','')
                date_object = datetime.strptime(date_as_string, '%Y-%m-%dT%H:%M:%S')
                date = date_object.timestamp()
                open = Bar_dict['o']
                high = Bar_dict['h']
                low = Bar_dict['l']
                close = Bar_dict['c']
                self.unformated_dataset.append(
                    [date,open,high,low,close]
                )

        if asset['data_provider'] == 'Binance':
            url_to_fetch = self.request_args.Binance(asset['historical_data_url'])
            response_raw = requests.get(url_to_fetch)
            self.unformated_dataset = response_raw.json()
            # Change the weird timestamps from Binanace to propper 
            # ones that actually work
            for ohlc in self.unformated_dataset:
                new_time_stamp = round(ohlc[0] / 1000)
                ohlc[0] = new_time_stamp

        if asset['data_provider'] == 'Kraken':
            url_to_fetch = self.request_args.Kraken(asset['historical_data_url'])
            response_raw = requests.get(url_to_fetch)
            json = response_raw.json()
            keys = list(json['result'].keys())
            self.unformated_dataset = json['result'][keys[0]]


    def OHLC_Price_List(self, asset):
        """
        It can be advantageous to determine whether data has been formated already
        This has the benefit of code readability wherein "format_data" is always called,
        but the system doesn't have to re-run the format if not required
        """

        self.get_historical_OHLC(asset)

        self.OHLC_list = []

        for element in self.unformated_dataset:
            op = float(element[1])
            hi = float(element[2])
            lo = float(element[3])
            cl = float(element[4])
            self.OHLC_list.append([element[0], op, hi, lo, cl])

        return self.OHLC_list
    
    def OHLC_Price_List_for_DB(self, asset, timeframe):
        # Get Asset Config Object
        self.get_historical_OHLC(asset)

        self.OHLC_list = []
        # Formate the Price Data List
        # Include Ticker and DataProvider 
        # if self.unformated_dataset == True:
        for element in self.unformated_dataset:
            open = float(element[1])
            high = float(element[2])
            low = float(element[3])
            close = float(element[4])
            
            # calculate average Price
            average = float(self.Average_Price(open,high,low,close))

            self.OHLC_list.append((
                element[0], 
                open, 
                high, 
                low, 
                close,
                average,
                asset['ticker'],
                asset['data_provider'],
                timeframe
            ))
        # Every ohlc row must be linked to the asset
        # so that we can identify it in our Database
        # one way would be to use the assets Table as an Index table
        # and we just use eachs Assets ID and link it to every OHLC row
        # or we do Ticker and Data_Provider as PK, but thats the 
        # unconveníent option
            
        return self.OHLC_list

    def Average_Price(self,open,high,low,close):
        add_all = open+high+low+close
        average = add_all/4
        return average



