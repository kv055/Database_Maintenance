import asyncio
import aiohttp
from datetime import datetime
from json import loads
import time

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
            
            if provider['data_provider'] == 'Alpaca':

                async def fetch_Alpaca_asset(asset_dict, index):
                    print(f"""{asset_dict[1],index}Test""")
                    ohlc_in_alpaca_raw_format = await self.Alpaca_API.get_OHLC(asset_dict[1],'1Day')
                    unformated_dataset = []
                    for Bar_dict in ohlc_in_alpaca_raw_format:
                        date_as_string = Bar_dict['t'].replace('Z','')
                        date_object = datetime.strptime(date_as_string, '%Y-%m-%dT%H:%M:%S')
                        date = date_object.timestamp()
                        open = Bar_dict['o']
                        high = Bar_dict['h']
                        low = Bar_dict['l']
                        close = Bar_dict['c']
                        unformated_dataset.append(
                            [date,open,high,low,close]
                        )
                    
                    unformated_price_data = {
                        'data_provider': asset_dict['data_provider'],
                        'ticker': asset_dict[1],
                        # 'timeframe': asset_dict['timeframe']
                        'OHLC_unformated': unformated_dataset
                    }

                    self.all_OHLC_data.append(unformated_price_data)
                    print('fetch Alpaca',index)
                    return unformated_price_data

                async def main():
                    tasks = [fetch_Alpaca_asset(asset_dict,index) for index,asset_dict in enumerate(provider['asset_dicts'])]
                    await asyncio.gather(*tasks)

                asyncio.run(main())              
  
            if provider['data_provider'] == 'Binance':
                
                async def fetch_Binance_url(asset_dict, index, session):
                    
                    async with session.get(f"""{asset_dict['historical_data_url']}1d""") as response:
                        # Timelock to not trigger the APIs rate limit of 1000 requests per minute
                        if index > 0 and 1000 / index == 1:
                            print('stopping requests for 2min to not trigger rate Limiter')
                            time.sleep(120)
                        
                        # Fetch and convert OHLC
                        answer = await response.text()
                        json = loads(answer)
                        
                        # Formate Timestamp from Binance to UNIX-Timestamp
                        for candle in json:
                            new_time_stamp = round(candle[0] / 1000)
                            candle[0] = new_time_stamp

                        unformated_price_data = {
                            'data_provider': asset_dict['data_provider'],
                            'ticker': asset_dict[1],
                            # 'timeframe': asset_dict['timeframe']
                            'OHLC_unformated': json
                        }
                        self.all_OHLC_data.append(unformated_price_data)
                        
                        print('fetch Binance',index)
                        return unformated_price_data

                async def main():
                    async with aiohttp.ClientSession() as session:
                        tasks = [fetch_Binance_url(asset_dict,index, session) for index,asset_dict in enumerate(provider['asset_dicts'])]
                        await asyncio.gather(*tasks)

                asyncio.run(main())
               
            if provider['data_provider'] == 'Kraken':
                
                async def fetch_Kraken_url(asset_dict, index, session):
                    async with session.get(f"""{asset_dict[2]}1440""") as response:
                        answer = await response.text()
                        json = loads(answer)
                        
                        unformated_price_data = {
                            'data_provider': asset_dict[0],
                            'ticker': asset_dict[1],
                            # 'timeframe': asset_dict['timeframe']
                            'OHLC_unformated': json['result'][asset_dict[1]]
                        }
                        self.all_OHLC_data.append(unformated_price_data)
                        
                        # print('fetch Kraken',index)
                        return unformated_price_data
                
                async def main():
                    async with aiohttp.ClientSession() as session:
                        tasks = [fetch_Kraken_url(asset_dict,index, session) for index,asset_dict in enumerate(provider['asset_dicts'])]
                        await asyncio.gather(*tasks)
                      
                asyncio.run(main())
    
    def OHLC_Price_List_for_DB(self):
        # Get Asset Config Object
        self.get_historical_OHLC()

        # Formate the Price Data List
        # Include Ticker and DataProvider 
        for asset in self.all_OHLC_data:
            asset['OHLC'] = []
            for candle in asset['OHLC_unformated']:
                open = float(candle[1])
                high = float(candle[2])
                low = float(candle[3])
                close = float(candle[4])
                
                # calculate average Price
                average = float(self.Average_Price(open,high,low,close))

                asset['OHLC'].append((
                    candle[0], 
                    open, 
                    high, 
                    low, 
                    close,
                    average,
                    asset['ticker'],
                    asset['data_provider'],
                    self.fetch_config['Candle_Size']
                ))

        return self.all_OHLC_data

    def Average_Price(self,open,high,low,close):
        add_all = open+high+low+close
        average = add_all/4
        return average