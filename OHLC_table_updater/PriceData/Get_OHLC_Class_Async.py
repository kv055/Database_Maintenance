import asyncio
import aiohttp
from datetime import datetime
import requests
from json import loads
import find_parent
from API_Connectors.AlpacaConnector import Alpaca
from OHLC_table_updater.PriceData.Request_URL_Generators import generate_request

# from Database_SQL.query_assets import return_all_asset_URLs
class Import_OHLC_Data_Async:
    """Fetching OHLC Data from various api's"""
    def __init__(self, ohlc_config, all_asset_dicts):
        self.fetch_config = ohlc_config
        self.request_args = generate_request(ohlc_config)
        # initialize Alpaca_Api connector instance
        self.Alpaca_API = Alpaca()
        self.asset_dicts_by_provider = all_asset_dicts

    def get_historical_OHLC(self):
        self.all_OHLC_data = []
        
        for provider in self.asset_dicts_by_provider:
            
            # if provider['data_provider'] == 'Alpaca':
            #     # Alpaca_to_fetch = self.request_args.Alpaca(asset['historical_data_url'])
            #     # url_to_fetch = Alpaca_to_fetch
            #     # req_body = Alpaca_to_fetch[1]
            #     ohlc_in_alpaca_raw_format = self.Alpaca_API.get_OHLC(asset['ticker'],'1Day')
            #     self.unformated_dataset = []
            #     for Bar_dict in ohlc_in_alpaca_raw_format:
            #         date_as_string = Bar_dict['t'].replace('Z','')
            #         date_object = datetime.strptime(date_as_string, '%Y-%m-%dT%H:%M:%S')
            #         date = date_object.timestamp()
            #         open = Bar_dict['o']
            #         high = Bar_dict['h']
            #         low = Bar_dict['l']
            #         close = Bar_dict['c']
            #         self.unformated_dataset.append(
            #             [date,open,high,low,close]
            #         )

            # filter assets by dataprovider
            if provider['data_provider'] == 'Binance':
                
                async def fetch_Binance_url(asset_dict, index, session):
                    async with session.get(f"""{asset_dict['historical_data_url']}1d""") as response:
                        answer = await response.text()
                        json = loads(answer)
                        for candle in json:
                            new_time_stamp = round(candle[0] / 1000)
                            candle[0] = new_time_stamp

                        unformated_price_data = {
                            'OHLC': answer,
                            'data_provider': asset_dict['data_provider'],
                            'ticker': asset_dict['ticker'],
                            # 'timeframe': asset_dict['timeframe']
                        }
                        
                        self.all_OHLC_data.append(unformated_price_data)
                        print('Binance',index)
                        return unformated_price_data

                async def main():
                    async with aiohttp.ClientSession() as session:
                        tasks = [fetch_Binance_url(asset_dict,index, session) for index,asset_dict in enumerate(provider['asset_dicts'])]
                        await asyncio.gather(*tasks)

                    asyncio.run(main())
               
            if provider['data_provider'] == 'Kraken':
                
                async def fetch_Kraken_url(asset_dict, index, session):
                    async with session.get(f"""{asset_dict['historical_data_url']}1440""") as response:
                        answer = await response.text()
                        json = loads(answer)
                        unformated_price_data = {
                            'OHLC': json['result'][asset_dict['ticker']],
                            'data_provider': asset_dict['data_provider'],
                            'ticker': asset_dict['ticker'],
                            # 'timeframe': asset_dict['timeframe']
                        }
                        self.all_OHLC_data.append(unformated_price_data)
                        print('Kraken',index)
                        return unformated_price_data
                
                async def main():
                    async with aiohttp.ClientSession() as session:
                        tasks = [fetch_Kraken_url(asset_dict,index, session) for index,asset_dict in enumerate(provider['asset_dicts'])]
                        await asyncio.gather(*tasks)
                      
                asyncio.run(main())
    
    def OHLC_Price_List_for_DB(self):
        # Get Asset Config Object
        self.get_historical_OHLC()

        self.formated_OHLC_data = []
        # Formate the Price Data List
        # Include Ticker and DataProvider 
        for asset in self.all_OHLC_data:
            OHLC_list = []
            for candle in asset['OHLC']:
                open = float(candle[1])
                high = float(candle[2])
                low = float(candle[3])
                close = float(candle[4])
                
                # calculate average Price
                average = float(self.Average_Price(open,high,low,close))

                OHLC_list.append((
                    candle[0], 
                    open, 
                    high, 
                    low, 
                    close,
                    average,
                    asset['ticker'],
                    asset['data_provider'],
                    # asset['timeframe']
                ))
            
            self.formated_OHLC_data.append(OHLC_list)
            
        return self.formated_OHLC_data

    def Average_Price(self,open,high,low,close):
        add_all = open+high+low+close
        average = add_all/4
        return average
