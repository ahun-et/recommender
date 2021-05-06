import os
import sys
import redis

from logger import logger
from blender import bcolors
from dotenv import load_dotenv
from pymongo import MongoClient

mongo = None
r = None

def connect():
    """ Connect to postgresql.
        On the event of no connection close the script.

        Returns
        -------
            void
    """
    global mongo, r

    load_dotenv()  # take environment variables from .env.

    try:
        print(f'{bcolors.OKBLUE}INFO: Connecting to the MongoDB databse... {bcolors.ENDC}')
        mongodbUrl = os.getenv('MONGODB_URL')
        client = MongoClient(mongodbUrl)
        mongo = client[os.getenv('MONGODB_DATABASE')]

        print(f'{bcolors.OKBLUE}INFO: Connecting to the Redis Client... {bcolors.ENDC}')
        r = redis.Redis(
            host = os.getenv('REDIS_HOST'),
            port = os.getenv('REDIS_PORT')
        )

        print(f'{bcolors.OKGREEN}SUCESS: Successful connected to MongoDB{bcolors.ENDC}')
        print(f'{bcolors.OKGREEN}SUCESS: Successful connected to Redis Client{bcolors.ENDC}')

    except Exception as ex:
        print(f'{bcolors.FAIL}ERROR: Failed to connect to Database{bcolors.ENDC}')
        #logger.exception(str(ex))
        print(ex)
        sys.exit(1)
