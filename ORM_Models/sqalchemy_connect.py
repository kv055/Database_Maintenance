
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine,MetaData

from dotenv import load_dotenv
import os

class SQL_Server:
    def __init__(self,DB_Name) -> None:
        # Load environment variables
        load_dotenv()
        ENDPOINT = os.getenv('AWSAURORA_ENDPOINT')
        USER = os.getenv('AWSAURORA_USER')
        ADMINPW = os.getenv('AWSAURORA_ADMINPW')
        PORT = os.getenv('AWSAURORA_PORT')
        # Create a database connection
        self.engine = create_engine('mysql+mysqlconnector://{user}:{adminpw}@{endpoint}:{port}/{dbname}'.format(
            user=USER,
            adminpw=ADMINPW,
            endpoint=ENDPOINT,
            port=PORT,
            dbname=DB_Name
        ))
        
    def get_base(self):
        # Define the base model
        self.Base = declarative_base()
        # Create the tables if they don't exist
        self.Base.metadata.create_all(self.engine)
        return self.Base

    def get_session(self):
        # Create a session
        self.Session = sessionmaker(bind=self.engine)
        session = self.Session()
        return session
    
    def get_metadata(self):
        metadata = MetaData(bind=self.engine)
        return metadata
    
    def get_engine(self):
        return self.engine