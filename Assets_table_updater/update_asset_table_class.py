from Assets_table_updater.get_all_asset_urls_from_API import all_listed_assets
from sqlalchemy import Column, Float, Table, Integer
# from sqlalchemy.schema import AddColumn
from sqlalchemy.sql import delete, insert, select

from ORM_Models.ORM_Models_Module import FinancialData

class update_asset_table:
    """
        Insert URL Data into Temp Table
         > left join temp & asset table = new data
         > inner join temp & asset table = update data
         > right join temp & asset table = delete data
    """
    def __init__(self):
        # establish connection to the Financiaal Data DB
        self.FinancialDataDb = FinancialData()
        self.Assets_Table = self.FinancialDataDb.return_Assets_Table()
        self.session = self.FinancialDataDb.session
        self.engine = self.FinancialDataDb.engine
        self.metadata = self.FinancialDataDb.metadata
        # Create Assets Table if it doesnts exist
        self.Assets_Table.create(checkfirst=True,bind=self.engine)

        # Check if id column exists and drop it
        if 'id' in self.Assets_Table.columns:
            self.Assets_Table.c.id.drop()
            self.Assets_Table.indexes.remove(self.Assets_Table.c.id_UNIQUE)

        assets_api_instance = all_listed_assets()
        self.Alpaca_URL_List = assets_api_instance.all_links_Alpaca
        self.Binance_URL_List = assets_api_instance.all_links_Binance
        self.Kraken_URL_List = assets_api_instance.all_links_Kraken
    
    def set_Alpaca_URL_List(self):
        self.data_set =  self.Alpaca_URL_List()
        self.data_provider = 'Alpaca'

    def set_Binance_URL_List(self):
        self.data_set =  self.Binance_URL_List()
        self.data_provider = 'Binance'

    def set_Kraken_URL_List(self):
        self.data_set =  self.Kraken_URL_List()
        self.data_provider = 'Kraken'

    def enter_into_db(self):

        # Reflect the schema of self.Assets_Table
        self.metadata.reflect(bind=self.engine, schema='Financial_Data')
        table_structure = self.Assets_Table.metadata.tables[self.Assets_Table.name]

        # Specify the name for your temporary table
        temp_table_name = 'Financial_Data'

        # Create the temporary table using the reflected table structure
        temp_assets_table = table_structure.tometadata(self.metadata, schema=temp_table_name)

        # Create the temporary table in the database
        temp_assets_table.create(self.engine, checkfirst=True)

        # Delete all rows from the temp table (in case it was populated earlier)
        delete_stmt = delete(temp_assets_table)
        self.session.execute(delete_stmt)   
        self.session.commit()

        # Perform the bulk INSERT operation
        self.session.execute(
            temp_assets_table.insert().values(self.data_set)
        )
        self.session.commit()

           # Add newly listed assets to the main table
        add_newly_listed_assets_stmt = (
            insert(self.Assets_Table)
            .from_select(
                self.Assets_Table.columns.keys(),
                select(temp_assets_table)
                .join(self.Assets_Table, (temp_assets_table.c.data_provider == self.Assets_Table.c.data_provider) &
                    (temp_assets_table.c.ticker == self.Assets_Table.c.ticker), isouter=True)
                .where(temp_assets_table.c.data_provider == self.data_provider)
                .where(self.Assets_Table.c.ticker.is_(None))
            )
        )
        self.session.execute(add_newly_listed_assets_stmt)
        self.session.commit()
        print(f'Inserted all assets from {self.data_provider}')

    
    def create_id_column(self):
        pass