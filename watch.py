import os
import sys
import time
import redis
import concurrent.futures

from logger import logger
from blender import bcolors
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()  # take environment variables from .env.

"""
    CONSTS
"""
REDIS_PREFIX = os.getenv('REDIS_PREFIX', 'usr:')
SLEEP_TIME = 3

def watchInsertVibes():
    """ Watch `vibes` collection """
    global REDIS_PREFIX, SLEEP_TIME

    mongo = None
    redis = None

    print(f'{bcolors.HEADER}\t\t\t\tPROCESS: Watch Insert Vibes{bcolors.ENDC}')

    try:
        print(f'{bcolors.OKBLUE}INFO: Connecting to the MongoDB databse... {bcolors.ENDC}')
        mongodbUrl = os.getenv('MONGODB_URL')
        client = MongoClient(mongodbUrl)
        mongo = client[os.getenv('MONGODB_DATABASE')]

        print(f'{bcolors.OKBLUE}INFO: Connecting to the Redis Client... {bcolors.ENDC}')
        redis = redis.Redis(
            host = os.getenv('REDIS_HOST'),
            port = os.getenv('REDIS_PORT')
        )

        print(f'{bcolors.OKGREEN}SUCESS: Successful connected to MongoDB{bcolors.ENDC}')
        print(f'{bcolors.OKGREEN}SUCESS: Successful connected to Redis Client{bcolors.ENDC}')

    except Exception as ex:
        print(f'{bcolors.FAIL}ERROR: Failed to connect to Database{bcolors.ENDC}')
        logger.exception(str(ex))
        sys.exit(1)

    print('\n\n')

    while True:
        time.sleep(SLEEP_TIME)
        try:
            for insert_change in mongo['vibes'].watch(
                [{'$match': {'operationType': 'insert'}}]
            ):
                # In order to avoid suggesting user's own vibe to user, treat the current user as a follower
                followers = [ insert_change['fullDocument']['user'] ]
                # User's followers
                for f in mongo['useredges'].find({'destination': insert_change['fullDocument']['user']}):
                    redis.lpush(REDIS_PREFIX + str(f['source']) + ':following', str(insert_change['fullDocument']['_id']))

                # Users that have interest in the vibe
                for f in mongo['users'].find({'_id': {'$nin': followers}, 'interests': {'$in': insert_change['fullDocument']['activityType']}}):
                    redis.lpush(REDIS_PREFIX + str(f['_id']) + ':suggested', str(insert_change['fullDocument']['_id']))

        except Exception as ex:
            logger.exception(ex)

def watchDeleteVibes():
    """ Watch `vibes` collection deletation """
    global REDIS_PREFIX, SLEEP_TIME

    mongo = None
    redis = None

    print(f'{bcolors.HEADER}\t\t\t\tPROCESS: Watch Delete Vibes{bcolors.ENDC}')

    try:
        print(f'{bcolors.OKBLUE}INFO: Connecting to the MongoDB databse... {bcolors.ENDC}')
        mongodbUrl = os.getenv('MONGODB_URL')
        client = MongoClient(mongodbUrl)
        mongo = client[os.getenv('MONGODB_DATABASE')]

        print(f'{bcolors.OKBLUE}INFO: Connecting to the Redis Client... {bcolors.ENDC}')
        redis = redis.Redis(
            host = os.getenv('REDIS_HOST'),
            port = os.getenv('REDIS_PORT')
        )

        print(f'{bcolors.OKGREEN}SUCESS: Successful connected to MongoDB{bcolors.ENDC}')
        print(f'{bcolors.OKGREEN}SUCESS: Successful connected to Redis Client{bcolors.ENDC}')

    except Exception as ex:
        print(f'{bcolors.FAIL}ERROR: Failed to connect to Database{bcolors.ENDC}')
        logger.exception(str(ex))
        sys.exit(1)

    print('\n\n')

    while True:
        try:
            for delete_change in mongo['vibes'].watch(
                [{'$match': {'operationType': 'delete'}}]
            ):
                # Loop throught every user and try removing the vibe id
                for f in redis.scan_iter(REDIS_PREFIX + '*'):
                    redis.lrem(f, 0, str(delete_change['documentKey']['_id']))

            # TODO: add snapshot in order to restart from the previous watch 
        except Exception as ex:
            # TODO: log the execption
            print(ex)


with concurrent.futures.ProcessPoolExecutor() as executor:
    executor.submit(watchInsertVibes)
    executor.submit(watchDeleteVibes)