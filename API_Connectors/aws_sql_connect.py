import os
import mysql.connector
# from mysql.connector import errorcode

class AWS_SQL:
    def __init__(self, environement):
        environement()
        ENDPOINT = os.getenv('AWSAURORA_ENDPOINT')
        PORT = os.getenv('AWSAURORA_PORT')
        REGION = os.getenv('AWSAURORA_REGION')
        self.DBNAME = os.getenv('AWSAURORA_DBNAME')
        USER = os.getenv('AWSAURORA_USER')
        ADMINPW = os.getenv('AWSAURORA_ADMINPW')
        
        self.connection = mysql.connector.connect(
            user=USER,
            password=ADMINPW,
            host=ENDPOINT,
            database=self.DBNAME
        )
        # connection.commit()

        self.cursor = self.connection.cursor()
        # cursor.execute()

    def close(self):
        self.connection.close()
    
    
class DummyData:
    def __init__(self, environement):
        environement()
        ENDPOINT = os.getenv('AWSAURORA_ENDPOINT')
        PORT = os.getenv('AWSAURORA_PORT')
        REGION = os.getenv('AWSAURORA_REGION')
        self.DBNAME = os.getenv('DUMMY_DATA_DBNAME')
        USER = os.getenv('AWSAURORA_USER')
        ADMINPW = os.getenv('AWSAURORA_ADMINPW')
        
        self.connection = mysql.connector.connect(
            user=USER,
            password=ADMINPW,
            host=ENDPOINT,
            database=self.DBNAME
        )
        # connection.commit()

        self.cursor = self.connection.cursor()
        # cursor.execute()

    def close(self):
        self.connection.close()
