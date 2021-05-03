import os
import redis
import concurrent.futures
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

REDIS_PREFIX = os.getenv('REDIS_PREFIX', 'user:')

# TODO: Remove duplicate vibes

def watchInsertVibes():
    """ Watch `vibes` collection """
    try:
        print('Connecting to the PostgreSQL databse...')
        mongodbUrl = os.getenv('MONGODB_URL')
        client = MongoClient(mongodbUrl)
        #db = client.ahunbackup
        db = client.ahuntest

        # Redis conneciton
        print('Connecting to the Redis databse...')
        r = redis.Redis(host=os.getenv('REDIS_HOST'), port=os.getenv('REDIS_PORT'))
    except pymongo.errors.ServerSelectionTimeoutError as err:
        print(err)
        sys.exit(1)

    # Listen to mongodb changes
    while True:
        try:
            for insert_change in db['vibes'].watch(
                [{'$match': {'operationType': 'insert'}}]
            ):
                # Conains id of followers
                # In order to avoid placing vibe to the posters user vibe treat the current user as a follower
                followers = [ insert_change['fullDocument']['user'] ]
                # Append at the top of all followers
                # Get user's followers
                for f in db['useredges'].find({'destination': insert_change['fullDocument']['user']}):
                    followers.append(f['source'])
                    # Get follower activity type
                    if db['users'].find({'_id': f['source'], 'interests': {'$in': insert_change['fullDocument'].get('activityType', [])}}).count() > 0:
                        r.lpush(REDIS_PREFIX + str(f['source']) + ':recommended-high', str(insert_change['fullDocument']['_id']))
                    else:
                        r.lpush(REDIS_PREFIX + str(f['source']) + ':recommended-medium', str(insert_change['fullDocument']['_id']))

                for f in db['users'].find({'_id': {'$nin': followers}}):
                    r.lpush(REDIS_PREFIX + str(f['_id']) + ':recommended-reserve', str(insert_change['fullDocument']['_id']))

                # TODO: add snapshot in order to restart from the previous watch 
        except Exception as ex:
            # TODO: log the execption
            print(ex)


def watchDeleteVibes():
    """ Watch `vibes` collection deletation """
    try:
        print('Connecting to the PostgreSQL databse...')
        mongodbUrl = os.getenv('MONGODB_URL')
        client = MongoClient(mongodbUrl)
        #db = client.ahunbackup
        db = client.ahuntest

        # Redis conneciton
        print('Connecting to the Redis databse...')
        r = redis.Redis(host=os.getenv('REDIS_HOST'), port=os.getenv('REDIS_PORT'))
    except pymongo.errors.ServerSelectionTimeoutError as err:
        print(err)
        sys.exit(1)

    while True:
        try:
            for delete_change in db['vibes'].watch(
                [{'$match': {'operationType': 'delete'}}]
            ):
                # Loop throught every user and try removing the vibe id
                for f in r.scan_iter(REDIS_PREFIX + '*'):
                    r.lrem(f, 0, str(delete_change['documentKey']['_id']))

            # TODO: add snapshot in order to restart from the previous watch 
        except Exception as ex:
            # TODO: log the execption
            print(ex)


def watchInsertUsers():
    """ Watch `users` collection and build recommendation for user """
    try:
        print('Connecting to the PostgreSQL databse... Watch Insert users')
        mongodbUrl = os.getenv('MONGODB_URL')
        client = MongoClient(mongodbUrl)
        #db = client.ahunbackup
        db = client.ahuntest

        # Redis conneciton
        print('Connecting to the Redis databse... Watching Insert users')
        r = redis.Redis(host=os.getenv('REDIS_HOST'), port=os.getenv('REDIS_PORT'))
    except pymongo.errors.ServerSelectionTimeoutError as err:
        print(err)
        sys.exit(1)

    while True:
        try:
            for insert_change in db['users'].watch(
                [{'$match': {'operationType': 'insert'}}]
            ):
                # Calculate user's vibe recommendation
                # User's interests
                interests = [f for f in insert_change['fullDocument'].get('interests', [])]

                # Get non blocked following
                following = [f['_id'] for f in db['useredges'].find({'source': insert_change['fullDocument']['_id'], 'request': 'FOLLOW'})]

                # Get not-seen vibes from followed user's and that are also user's interests
                #vibes_followed_interests = [f['_id'] for f in db['vibes'].find({'_id': {'$nin': seen_vibes}, 'user': {'$in': following}, 'activityType': {'$in': interests}})]
                vibes_followed_interests = []

                for f in db['vibes'].find({'user': {'$in': following}, 'activityType': {'$in': interests}}):
                    vibes_followed_interests.append(f['_id'])
                    #r.lrem(str(user['_id']) + ':recommended-high', 0, str(f['_id']))
                    r.lpush(REDIS_PREFIX + str(insert_change['fullDocument']['_id']) + ':recommended-high', str(f['_id']))

                # Get vibes that are based on users interests
                #vibes_interests = [f['_id'] for f in db['vibes'].find({'_id': {'$nin': seen_vibes + vibes_followed_interests + vibes_followed}, 'activityType': {'$in': interests}})]
                vibes_interests = []

                for f in db['vibes'].find({'_id': {'$nin': vibes_followed_interests}, 'activityType': {'$in': interests}}):
                    vibes_interests.append(f['_id'])
                    # TODO: Remove andy redundent data if found on redis
                    r.lpush(REDIS_PREFIX + str(insert_change['fullDocument']['_id']) + ':recommended-medium', str(f['_id']))

                # # Get vibes that are not in interests
                #vibes_followed = [f['_id'] for f in db['vibes'].find({'_id': {'$nin': seen_vibes + vibes_followed_interests}, 'user': {'$in': following}})]
                vibes_followed = []

                for f in db['vibes'].find({'_id': {'$nin': vibes_followed_interests + vibes_interests}, 'user': {'$in': following}}):
                    vibes_followed.append(f['_id'])
                    # TODO: Remove andy redundent data if found on redis
                    r.lpush(REDIS_PREFIX + str(insert_change['fullDocument']['_id']) + ':recommended-medium', str(f['_id']))

                # Reserved vibes
                #other_vibes = [f['_id'] for f in db['vibes'].find({'_id': {'$nin': seen_vibes + vibes_followed_interests + vibes_followed + vibes_interests}})]

                for f in db['vibes'].find({'_id': {'$nin': vibes_followed_interests + vibes_followed + vibes_interests}}):
                    #r.lrem(str(user['_id']) + ':recommended-reserve', 0, str(f['_id']))
                    r.lpush(REDIS_PREFIX + str(insert_change['fullDocument']['_id']) + ':recommended-reserve', str(f['_id']))

        except Exception as ex:
            # TODO: log the execption
            print(ex)


def watchUpdateUsers():
    """ watch `users` collection and re-build recommendation for user based on the interest """
    pass


def watchVibeseen():
    """ Watch `vibeseen` collection and remove vibe from redis """
    while True:
        try:
            for insert_change in db['vibes'].watch(
                [{'$match': {'operationType': 'insert'}}]
            ):
                calculateVibeWeight(insert_change)
        except pymongo.errors.PyMongoError as ex:
            # TODO: log the execption
            print(ex)

def watchInsertUseredges():
    """ Watch `useredges` collection and build recommendation based on that """
    try:
        print('Connecting to the PostgreSQL databse... Watch Insert users')
        mongodbUrl = os.getenv('MONGODB_URL')
        client = MongoClient(mongodbUrl)
        #db = client.ahunbackup
        db = client.ahuntest

        # Redis conneciton
        print('Connecting to the Redis databse... Watching Insert users')
        r = redis.Redis(host=os.getenv('REDIS_HOST'), port=os.getenv('REDIS_PORT'))
    except pymongo.errors.ServerSelectionTimeoutError as err:
        print(err)
        sys.exit(1)

    while True:
        try:
            for insert_change in db['useredges'].watch(
                [{'$match': {'operationType': 'insert'}}]
            ):
                seen_vibes = [v['_id'] for v in db['vibeseens'].find({'userId': insert_change['fullDocument']['source']})]
                user = db['users'].find_one({'_id': insert_change['fullDocument']['source']})
                
                # Get the followed user vibes
                for f in db['vibes'].find({'_id': {'$nin': seen_vibes}, 'user': insert_change['fullDocument']['destination']}):
                    h = False
                    # In case of the followed user's vibe is already recommended 
                    # remove the old recommendation and replicate with the new
                    r.lrem(REDIS_PREFIX + str(user['_id']) + ':recommended-high', 0, str(f['_id']))
                    r.lrem(REDIS_PREFIX + str(user['_id']) + ':recommended-medium', 0, str(f['_id']))
                    r.lrem(REDIS_PREFIX + str(user['_id']) + ':recommended-reserve', 0, str(f['_id']))
                    
                    # If any of the activity type match user's interests recommend as high
                    if 'interests' in user and f.get('activityType', []) != []:
                        for a in f['activityType']:
                            if a in user['interests']:
                                r.lpush(REDIS_PREFIX + str(user['_id']) + ':recommended-high', str(f['_id']))
                                h = True
                                break
            
                    if h == False:
                        r.lpush(REDIS_PREFIX + str(user['_id']) + ':recommended-medium', str(f['_id']))
                        

        except Exception as ex:
            # TODO: log the execption
            print(ex)


with concurrent.futures.ProcessPoolExecutor() as executor:
    executor.submit(watchInsertVibes)
    executor.submit(watchDeleteVibes)
    executor.submit(watchInsertUsers)
    executor.submit(watchInsertUseredges)