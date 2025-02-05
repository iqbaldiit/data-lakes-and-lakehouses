# config_loader.py

'''
This class is design to :
    1. Load environment variables from a .env file and system environment variables.
    2. Provide a structured way to access configuration values like AWS credentials 
        and file paths, which are crucial for the ingestion layer to connect to data 
        sources, process data, and store it.                
'''


import os  # Provides utilities for interacting with the operating system,such as file paths and environment variables 
from dotenv import load_dotenv #Loads environment variables from a .env file into the system environment for easy access.


class ConfigLoader:
    def __init__(self):
        # Load .env file
        # path= current file directory, 2 steps back, file (/util/ingestion/.env)
        dotenv_path = os.path.join(os.path.dirname(__file__), '../..', '.env')
        load_dotenv(dotenv_path, override=True)
        # override=True: Ensures that .env variables replace any existing environment variables with the same name.
        
        # Load environment variables from .env file
        self.aws_s3_endpoint = os.environ.get("AWS_S3_ENDPOINT")
        self.aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
        self.aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
        self.lakehouse_s3_path = os.environ.get("LAKEHOUSE_S3_PATH")

        # Define the paths to local files
        self.script_dir = os.path.dirname(os.path.abspath(__file__))
        self.base_data_dir = os.path.join(self.script_dir, '../..', 'data')

        # Define connection to MSSQL
        self.mssql_uri=os.environ.get("MSSQL_URL")
        self.mssql_properties={
            "user": os.getenv("MSSQL_USER"),
            "password": os.getenv("MSSQL_PASSWORD"),
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver" # Note
        }

        # Define connection to PGSQL
        self.pgsql_uri=os.environ.get("PGSQL_URL")
        self.pgsql_properties={
            "user": os.getenv("PGSQL_USER"),
            "password": os.getenv("PGSQL_PASSWORD"),
            "driver": "org.postgresql.Driver"# Note
        } 

        # Define connection to Mongo DB
        self.mongo_uri=os.environ.get("MONGO_URI")       

'''
self.script_dir: Absolute path to the directory containing the script. This ensures that paths 
                are resolved correctly regardless of where the code is executed.

os.path.abspath(__file__): Converts the relative script path to an absolute one.

os.path.dirname: Extracts the directory part of the script path.

self.base_data_dir: Defines a base directory for storing local data files.
                    Combines the script directory with ../.. and a folder named data to form 
                    a standard local storage path.        
'''