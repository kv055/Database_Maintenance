# import find_parent

from dotenv import load_dotenv
from API_Connectors.aws_sql_connect import AWS_SQL, DummyData

from Get_All_Assets.Alpaca_assets import all_links_Alpaca
from Get_All_Assets.Binance_assets import all_links_Binance
from Get_All_Assets.Kraken_assets import all_links_Kraken

# establish connection to the Dummy Data DB
db_connection = DummyData(load_dotenv)
"""
def insert_user(connector):
    # Data
    user = {
        'first_name': 'Kilian',
        'last_name': 'Voss',
        'e_mail': 'kilian96@live.de'
    }
    ins = f"INSERT INTO users (first_name, last_name, e_mail) VALUES (%s,%s,%s)"
    val = (user['first_name'],user['last_name'],user['e_mail'])
    connector.cursor.execute(ins, val)
    connector.connection.commit()
"""
def insert_update_Kraken_rows():
    #get the data to insert
    Kraken_URIs = all_links_Kraken()
    insert_urls_to_db('kraken', Kraken_URIs)
    # insert into DummyData.kraken_assets
    # insert into DummyData.assets

def insert_update_Binance_rows():
    #get the data to insert
    Binance_URIs = all_links_Binance()
    insert_urls_to_db('binance', Binance_URIs)
    # inser into DummyData.binance_assets
    # insert into DummyData.assets

def insert_update_Aplaca_rows():
    #get the data to insert
    Alpaca_Assets = all_links_Alpaca()
    # insert into DummyData.alpaca_assets
    # insert into DummyData.assets

def insert_urls_to_db(exchange, dataSet):
    """
        Insert URL Data into Temp Table
         > left join temp & asset table = new data
         > inner join temp & asset table = update data
         > right join temp & asset table = delete data
    """
    # check which exchange and handle accordingly 
    if exchange == 'kraken':
        # hold all new data 
        db_connection.cursor.execute("CREATE TABLE IF NOT EXISTS new_urls LIKE kraken_assets")
        db_connection.connection.commit()
        db_connection.cursor.execute("DELETE FROM new_urls WHERE id")
        db_connection.connection.commit()
        for id, data in enumerate(dataSet):
            vals = (id+1, data[0], data[2], data[3])
            insertQuery = f"INSERT INTO new_urls(id, ticker, historical_data_url, live_data_url) VALUES ({vals[0]}, '{vals[1]}','{vals[2]}','{vals[3]}')"
            db_connection.cursor.execute(insertQuery) 
            db_connection.connection.commit()
            # if not id  10:
            #     break
        db_connection.connection.commit()
        db_connection.cursor.execute("SELECT * FROM new_urls;")
        # db_connection.connection.commit()
        tmpTable = db_connection.cursor.fetchall()
    elif exchange == "binace": 
        # new tables have different 
        db_connection.cursor.execute("CREATE TABLE IF NOT EXISTS new_urls LIKE binance_assets")
        # update current data
        # delete old data
    elif exchange == "alpaca":
        db_connection.cursor.execute("CREATE TABLE IF NOT EXISTS new_urls LIKE alpaca_assets")
        
    # -------- temp data has been made by this point ----
    db_connection.cursor.execute(f"select * from {exchange}_assets RIGHT JOIN new_urls on {exchange}_assets.ticker = new_urls.ticker")
    db_connection.connection.commit()
    # deletedData = fetchall()
    # db_connection.cursor.execute(f"delete from {exchange}_assets where ... in {deletedData}")



# alpaca = insert_update_Aplaca_rows()
krk = insert_update_Kraken_rows()
binance = insert_update_Binance_rows()
print("thing")

