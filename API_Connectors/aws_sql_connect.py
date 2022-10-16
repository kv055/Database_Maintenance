import os

import mysql.connector
from dotenv import load_dotenv

# from mysql.connector import errorcode


class SQL_Server:
    def __init__(self, DB_Name = None):
        load_dotenv()
        self.ENDPOINT = os.getenv('AWSAURORA_ENDPOINT')
        self.USER = os.getenv('AWSAURORA_USER')
        self.ADMINPW = os.getenv('AWSAURORA_ADMINPW')
        
        if DB_Name == None:
            self.connection = mysql.connector.connect(
                user = self.USER,
                password = self.ADMINPW,
                host = self.ENDPOINT
            )
        else:
            self.connection = mysql.connector.connect(
            user = self.USER,
            password = self.ADMINPW,
            host = self.ENDPOINT,
            database = DB_Name
            )
        
        self.cursor = self.connection.cursor(dictionary=True)

    def connect_db(self,db_name):
        self.connection = mysql.connector.connect(
            user = self.USER,
            password = self.ADMINPW,
            host = self.ENDPOINT,
            database = db_name
        )
        self.cursor = self.connection.cursor(dictionary=True)

    def close(self):
        self.connection.close() 
        # self.db_connection.close() 
