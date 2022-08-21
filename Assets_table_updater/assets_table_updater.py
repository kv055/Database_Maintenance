# import find_parent

from dotenv import load_dotenv
from API_Connectors.aws_sql_connect import AWS_SQL, DummyData

from Get_All_Assets.Alpaca_assets import all_links_Alpaca
from Get_All_Assets.Binance_assets import all_links_Binance
from Get_All_Assets.Kraken_assets import all_links_Kraken

# establish connection to the Dummy Data DB
db_connection = DummyData(load_dotenv)

def insert_update_Kraken_rows():
    #get the data to insert
    Kraken_URIs = all_links_Kraken()
    insert_urls_to_db('kraken', Kraken_URIs)
    # insert into DummyData.kraken_assets
    # insert into DummyData.assets

def insert_update_Binance_rows():
    #get the data to insert
    Binance_URIs = all_links_Binance()
    insert_urls_to_db(Binance_URIs)
    # inser into DummyData.binance_assets
    # insert into DummyData.assets

def insert_update_Aplaca_rows():
    #get the data to insert
    Alpaca_Assets = all_links_Alpaca()
    # insert into DummyData.alpaca_assets
    # insert into DummyData.assets

def insert_urls_to_db(dataSet):
    """
        Insert URL Data into Temp Table
         > left join temp & asset table = new data
         > inner join temp & asset table = update data
         > right join temp & asset table = delete data
    """
    
    # create temp table to hold all new data 
    db_connection.cursor.execute("CREATE TABLE IF NOT EXISTS new_urls LIKE assets")
    db_connection.connection.commit()
    # no clue what that does
    db_connection.cursor.execute("DELETE FROM new_urls WHERE id")
    db_connection.connection.commit()
    # insert all new records into the temp table
    # insert_sql = "INSERT INTO new_urls (ticker, data_provider, historical_data_url, live_data_url) VALUES (%s,%s,%s,%s)"
    # db_connection.cursor.executemany(insert_sql, dataSet)
    db_connection.connection.commit()

    # check if the assets table has records that are not in the temp table
    get_number_of_delisted_assets_sql = f"""
        select count(*) from new_urls
        right join binance_assets on new_urls.ticker = binance_assets.ticker
        where new_urls.id is null;
            
    """
    db_connection.cursor.execute(get_number_of_delisted_assets_sql)
    number_of_delisted_assets = db_connection.cursor.fetchall()

    if number_of_delisted_assets > 0:
        # get asset table records that are not in the temp table
        get_PK_from_delisted_assets_sql = f"""
            select * from new_urls
            right join binance_assets on new_urls.ticker = binance_assets.ticker
            where new_urls.id is null;   
        """
        db_connection.cursor.execute(get_PK_from_delisted_assets_sql)
        PK_from_delisted_assets = db_connection.cursor.fetchall()
        # delete rows that are not in the temp table
        for row in PK_from_delisted_assets:
            ticker = row[6]
            delete_delisted_row_sql = f"""
                DELETE FROM binance_assets WHERE ticker = '{ticker}'
            """
            db_connection.cursor.execute(delete_delisted_row_sql)
            db_connection.connection.commit()

    # check if the temp table has records that are not in the assets table
    get_number_of_newly_listed_assets_sql = f"""
        select count(*) from new_urls
        left join binance_assets on new_urls.ticker = binance_assets.ticker
        where binance_assets.live_data_url <> new_urls.live_data_url 
            or binance_assets.id is null;
    """
    db_connection.cursor.execute(get_number_of_newly_listed_assets_sql)
    number_of_newly_listed_assets = db_connection.cursor.fetchall()

    if number_of_newly_listed_assets > 0:
        insert_newly_listed_assets_sql = f"""
                insert into kraken_assets
            select 
            null, new_urls.ticker, new_urls.historical_data_url, new_urls.live_data_url 
            from new_urls
            left join kraken_assets on new_urls.ticker = kraken_assets.ticker
            where kraken_assets.live_data_url <> new_urls.live_data_url 
                or kraken_assets.id is null ;
        """
        db_connection.cursor.executemany(insert_newly_listed_assets_sql)
        db_connection.connection.commit()
    
    l = 0
    

# alpaca = insert_update_Aplaca_rows()
# krk = insert_update_Kraken_rows()
binance = insert_update_Binance_rows()
print("thing")

