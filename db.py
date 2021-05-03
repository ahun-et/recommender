import os
import sys

from dotenv import load_dotenv
from pymongo import MongoClient

## Load env file
load_dotenv()

def connect():
    try:
        mongodbUrl = os.getenv('MONGODB_URL')
        client = MongoClient(mongodbUrl)
        return client.ahun_database
    except Exception as ex:
        print(ex)
        sys.exit(0)