import find_parent
from Assets_table_updater.get_all_asset_urls_from_API import all_listed_assets


class Get_all_assets:
    """
        Insert URL Data into Temp Table
         > left join temp & asset table = new data
         > inner join temp & asset table = update data
         > right join temp & asset table = delete data
    """
    def __init__(self):
        assets_api_instance = all_listed_assets()
        # self.Alpaca_URL_List = assets_api_instance.all_links_Alpaca
        # self.Binance_URL_List = assets_api_instance.all_links_Binance
        self.Kraken_URL_List = assets_api_instance.all_links_Kraken
    
    # def set_Alpaca_URL_List(self):
    #     self.Alpaca_assets =  self.Alpaca_URL_List()
    #     return self.Alpaca_assets

    # def set_Binance_URL_List(self):
    #     self.Binance_assets =  self.Binance_URL_List()
    #     return self.Binance_assets

    def set_Kraken_URL_List(self):
        self.Kraken_assets =  self.Kraken_URL_List()
        return self.Kraken_assets

