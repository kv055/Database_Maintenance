from dotenv import load_dotenv
import requests
from API_Connectors.aws_sql_connect import DummyData
from random import randint, choice
# Instanciate DB Connection
connector = DummyData(load_dotenv)

# Generate fake Users
# Fetch dummy data
# https://retool.com/utilities/generate-api-from-mock-data
# users_answer = requests.get("https://retoolapi.dev/UpZEES/data")
# users_answer_json = users_answer.json()
userDB_user_id = []

# config
configDB = []
# keys DB
keysDB = []

for index in range(50):
    # Users DB
    # userDB_user_id.append(index)

    # Config DB
    random_user_id = randint(7,57)

    configDB_strategy_id = 1
    configDB_asset_id = randint(2500,4000)
    configDB.append((random_user_id,configDB_strategy_id,configDB_asset_id))

    # Keys DB
    keysDB_exchange_name = choice(['Binance', 'Alpaca'])
    # keysDB_exchange_id = 69
    keysDB_pub_key = 5984989897
    keysDB_priv_key = 56925786567534
    keysDB.append((random_user_id,keysDB_exchange_name,keysDB_pub_key,keysDB_priv_key))
    

# # Insert fake Users
# insert_users = []
# ins = f"""INSERT INTO users (first_name, last_name, e_mail) VALUES (%s,%s,%s)"""
# for row in users_answer_json:
#     first = row['fullName']
#     last = row['rating']
#     e_mail = row['col1']
#     insert_users.append((first, last, e_mail))

# connector.cursor.executemany(ins, insert_users)
# connector.connection.commit()

# Generate fake config



# Insert fake Config

# ins = f"""INSERT INTO config_live_trading (user_id,strategy_id,asset_id) VALUES (%s,%s,%s)"""
# connector.cursor.executemany(ins, configDB)
# connector.connection.commit()

# Generate fake keys

# insert fake Keys

insert_keys = []
ins = f"""INSERT INTO exchange_keys (user_id, exchange_name, pub_key, priv_key) VALUES (%s,%s,%s,%s)"""
connector.cursor.executemany(ins, keysDB)
connector.connection.commit()