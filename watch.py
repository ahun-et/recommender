import os
import sys
import time
import redis
import pymongo
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
            logger.exception(str(ex))

def watchInsertUsers():
    """ Watch `users` collection and build recommendation for user """
    global REDIS_PREFIX, SLEEP_TIME

    mongo = None
    redis = None

    print(f'{bcolors.HEADER}\t\t\t\tPROCESS: Watch Insert Users{bcolors.ENDC}')

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
            for insert_change in mongo['users'].watch(
                [{'$match': {'operationType': 'insert'}}]
            ):
                # User's interests
                interests = [f for f in insert_change['fullDocument'].get('interests', [])]

                # Get non blocked following
                following = [f['_id'] for f in mongo['useredges'].find({'source': insert_change['fullDocument']['_id'], 'request': 'FOLLOW'})]

                # Vibes of following users
                vibes_followed = []

                for f in mongo['vibes'].find({'user': {'$in': following}}).sort('created_at', pymongo.DESCENDING):
                    vibes_followed.append(f['_id'])
                    redis.lpush(REDIS_PREFIX + str(insert_change['fullDocument']['_id']) + ':following', str(f['_id']))

                # Get vibes that are based on users interests    
                vibes_interests = []

                for f in mongo['vibes'].find({'_id': {'$nin': vibes_followed}, 'activityType': {'$in': interests}}).sort('created_at', pymongo.DESCENDING):
                    vibes_interests.append(f['_id'])
                    # TODO: Remove andy redundent data if found on redis
                    redis.lpush(REDIS_PREFIX + str(insert_change['fullDocument']['_id']) + ':suggested', str(f['_id']))

        except Exception as ex:
            logger.exception(str(ex))

def watchInsertUseredges():
    """ Watch `useredges` collection and build recommendation based on that """
    global REDIS_PREFIX, SLEEP_TIME

    mongo = None
    redis = None

    print(f'{bcolors.HEADER}\t\t\t\tPROCESS: Watch Insert User edges{bcolors.ENDC}')

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
            for insert_change in db['useredges'].watch(
                [{'$match': {'operationType': 'insert'}}]
            ):
                seen_vibes = [v['_id'] for v in mongo['vibeseens'].find({'userId': insert_change['fullDocument']['source']})]
                user = mongo['users'].find_one({'_id': insert_change['fullDocument']['source']})

                # Get the followed user vibes
                for f in mongo['vibes'].find({'_id': {'$nin': seen_vibes}, 'user': insert_change['fullDocument']['destination']}).sort('created_at', pymongo.DESCENDING):
                    h = False
                    # In case of the followed user's vibe is already recommended 
                    # remove the old recommendation and replicate with the new
                    redis.lrem(REDIS_PREFIX + str(user['_id']) + ':following', 0, str(f['_id']))
                    redis.lrem(REDIS_PREFIX + str(user['_id']) + ':suggested', 0, str(f['_id']))

                    # If any of the activity type match user's interests recommend as high
                    if 'interests' in user and f.get('activityType', []) != []:
                        for a in f['activityType']:
                            if a in user['interests']:
                                redis.lpush(REDIS_PREFIX + str(user['_id']) + ':suggested', str(f['_id']))
                                h = True
                                break

                    if h == False:
                        redis.lpush(REDIS_PREFIX + str(user['_id']) + ':following', str(f['_id']))

        except Exception as ex:
            logger.exception(str(ex))

with concurrent.futures.ProcessPoolExecutor() as executor:
    executor.submit(watchInsertVibes)
    executor.submit(watchDeleteVibes)
    executor.submit(watchInsertUsers)
    executor.submit(watchInsertUseredges)