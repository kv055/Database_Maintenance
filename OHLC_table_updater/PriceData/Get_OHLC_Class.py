import find_parent
from datetime import datetime
import requests
import numpy
import talib
import pandas as pd

from PriceData.Request_URL_Generators import generate_request
from API_Connectors.AlpacaConnector import Alpaca
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
            
            self.OHLC_list.append((
                element[0], 
                open, 
                high, 
                low, 
                close,
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

    def Average_Price_List(self,asset):
        """Method"""
        self.get_historical_OHLC(asset)
        
        self.create_numpy_array()

        avg_price_instance = talib.AVGPRICE(
            self.numpy_array[1], 
            self.numpy_array[2], 
            self.numpy_array[3], 
            self.numpy_array[4]
            ).tolist()

        self.average_price = []

        for averaged_price in avg_price_instance:
            self.average_price.append(averaged_price)

        return self.average_price

    def create_numpy_array(self):
        """Method"""
        # creating empty lists
        date = []
        op = []
        hi = []
        lo = []
        cl = []

        # pushing data into lists
        for element in self.OHLC_list:
            date.append(element[0])
            op.append(element[1])
            hi.append(element[2])
            lo.append(element[3])
            cl.append(element[4])

        # converting list to array (for talib)
        
        open = numpy.array(op)
        high = numpy.array(hi)
        low = numpy.array(lo)
        close = numpy.array(cl)

        self.numpy_array = [
            date,
            open,
            high,
            low,
            close
        ]

    def Create_Pandas_Dataframe(self):
        self.OHLC_Price_List()
        self.PriceDataFrame = pd.Single_DataFrame(
            self.OHLC_list
            ,columns=(
            'TimeStamp','Open','High','Low','Close'
            # ,'Volume','Close time',
            # 'Quote asset volume','Number of trades','Taker buy base asset volume',
            # 'Taker buy quote asset volume','ignore'
            )
        )
    
    def Pandas_Dataframe_Average_Price(self, name):
        
        self.create_pandas_dataframe()

        # self.PriceDataFrame['TimeStamp']= pd.to_datetime(self.PriceDataFrame['TimeStamp'], unit='ms')
        # self.PriceDataFrame.set_index("TimeStamp", inplace = True)
        self.PriceDataFrame["Close"] = pd.to_numeric(self.PriceDataFrame["Close"])

        del self.PriceDataFrame['Open']
        del self.PriceDataFrame['High']
        del self.PriceDataFrame['Low']
        # del self.PriceDataFrame['Volume']
        # del self.PriceDataFrame['Close time']
        # del self.PriceDataFrame['Quote asset volume']
        # del self.PriceDataFrame['Number of trades']
        # del self.PriceDataFrame['Taker buy base asset volume']
        # del self.PriceDataFrame['Taker buy quote asset volume']
        # del self.PriceDataFrame['ignore']

        self.PriceDataFrame.rename(columns = {'Close':name}, inplace = True) 
        return self.PriceDataFrame

    def Pandas_Dataframe_OHLC_Price(self):

        self.create_pandas_dataframe()

        change_date_from_timestamp = pd.to_datetime(self.PriceDataFrame['TimeStamp'], unit='ms')
        self.PriceDataFrame['TimeStamp'] = change_date_from_timestamp 
        # pd.to_datetime(self.PriceDataFrame['TimeStamp'], unit='ms')
        self.PriceDataFrame.set_index("TimeStamp", inplace = True)
    
        self.PriceDataFrame['Open'] = pd.to_numeric(self.PriceDataFrame['Open'])
        self.PriceDataFrame['High'] = pd.to_numeric(self.PriceDataFrame['High'])
        self.PriceDataFrame['Low'] = pd.to_numeric(self.PriceDataFrame['Low'])
        self.PriceDataFrame['Close'] = pd.to_numeric(self.PriceDataFrame["Close"])    

        return self.PriceDataFrame


# class ImportData:
#     """Class"""
#     def __init__(self):
#         self.bin_url = 'https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1d&limit=70'
#         self.OHLC_list = []
#         self.json = None
#         self.connect_data()
#         self.is_OHLC_list = False

#     def connect_data(self, url=None):
#         """if we want to load data other than the url specified in __init__"""
#         if url is not None:
#             self.bin_url = url
#         response = requests.get(self.bin_url)
#         self.json = response.json()
#         # new data has been loaded, thus not OHLC_list
#         self.is_OHLC_list = False

#     def OHLC_Price_List(self):
#         """
#         It can be advantageous to determine whether data has been OHLC_list already
#         This has the benefit of code readability wherein "format_data" is always called,
#         but the system doesn't have to re-run the format if not required
#         """
#         if self.is_OHLC_list:
#             return True

#         for element in self.json:
#             op = round(float(element[1]), 3)
#             hi = round(float(element[2]), 3)
#             lo = round(float(element[3]), 3)
#             cl = round(float(element[4]), 3)
#             # op = float(element[1])
#             # hi = float(element[2])
#             # lo = float(element[3])
#             # cl = float(element[4])
#             self.OHLC_list.append([round(element[0] / 1000), op, hi, lo, cl])
#         self.is_OHLC_list = True
#         return True

#     def create_numpy_array(self):
#         """Method"""
#         # creating empty lists
#         date = []
#         op = []
#         hi = []
#         lo = []
#         cl = []

#         # pushing data into lists
#         for element in self.OHLC_list:
#             date.append(element[0])
#             op.append(element[1])
#             hi.append(element[2])
#             lo.append(element[3])
#             cl.append(element[4])

#         # converting list to array (for talib)
        
#         open = numpy.array(op)
#         high = numpy.array(hi)
#         low = numpy.array(lo)
#         close = numpy.array(cl)

#         self.numpy_array = [
#             date,
#             open,
#             high,
#             low,
#             close
#         ]
    
#     def get_format_data(self):
#         """Method"""
#         if len(self.OHLC_list) < 1:
#             self.format_data()
#         return self.OHLC_list

#     def reset_format_data(self):
#         """Method"""
#         self.OHLC_list = []

#     def Average_Price_List(self):
#         """Method"""
#         self.create_numpy_array()

#         avg_price_instance = talib.AVGPRICE(
#             self.numpy_array[1], 
#             self.numpy_array[2], 
#             self.numpy_array[3], 
#             self.numpy_array[4]
#             ).tolist()

#         self.average_price = []

#         for averaged_price in avg_price_instance:
#             self.average_price.append(averaged_price)

#         return self.average_price

    # def get_average(self):
    #     """Method"""
    #     # if len(self.average[0]) == 0 or len(self.average[1]) == 0:
    #     #     self.calc_average()
    #     return self.average_price


# Formated in Lists
# Formated in Dict

