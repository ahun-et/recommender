import os
import sys
import time
import redis
import json
from dotenv import load_dotenv
from pymongo import MongoClient
from resource import getrusage, RUSAGE_SELF

load_dotenv()

"""
    Interests Score
"""
DEFAULT_INTEREST_WEIGHT = 0.1
FOLLOW_WEIGHT = 0.1
DEFAULT_WEIGHT = 0.1
FOLLOWING_WEIGHT = 0.25
FOLLOWING_INTEREST_WEIGHT = 0.5
REDIS_PREFIX = os.getenv('REDIS_PREFIX', 'u_')

try:
    print('Connecting to the PostgreSQL databse...')
    mongodbUrl = os.getenv('MONGODB_URL')
    client = MongoClient(mongodbUrl)
    #db = client.ahunbackup
    db = client.ahuntest

    # Redis conneciton
    print('Connecting to the Redis databse...')
    r = redis.Redis(host=os.getenv('REDIS_HOST'), port=os.getenv('REDIS_PORT'))
except Exception as err:
    print(err)
    sys.exit(1)

def percentage(part, whole):
    Percentage = 100 * int(part)/int(whole)
    return str(Percentage) + '%'

i = 1

start = time.perf_counter()
"""
    Start all mesurable process
"""

users_collection = db['users']
# TODO: To optimize select users that have followers
users = users_collection.find({})

for user in users:
    print(f' ***************** {i}')
    i += 1

    follow_weight = FOLLOW_WEIGHT
    total_weight = 0
    recommended_vibes = [] # contains follwing's not seen vibes
    
    # User's interests
    interests = [f for f in user.get('interests', [])]
    
    # Get user's already seen vibes
    seen_vibes = [v['_id'] for v in db['vibeseens'].find({'userId': user['_id']})]
    #seen_vibes = list(db['vibeseens'].find({'userId': user['_id']}))
    # In order to avoid user's own vibe to user, treat users vibe as seen
    for f in db['vibes'].find({'user': user['_id']}):
        seen_vibes.append(f['_id'])
    
    # Get non blocked following
    following = [f['_id'] for f in db['useredges'].find({'source': user['_id'], 'request': 'FOLLOW'})]

    # Get not-seen vibes from followed user's and that are also user's interests
    #vibes_followed_interests = [f['_id'] for f in db['vibes'].find({'_id': {'$nin': seen_vibes}, 'user': {'$in': following}, 'activityType': {'$in': interests}})]
    vibes_followed_interests = []

    for f in db['vibes'].find({'_id': {'$nin': seen_vibes}, 'user': {'$in': following}, 'activityType': {'$in': interests}}):
        vibes_followed_interests.append(f['_id'])
        #r.lrem(str(user['_id']) + ':recommended-high', 0, str(f['_id']))
        r.lpush(REDIS_PREFIX + str(user['_id']) + ':recommended-high', str(f['_id']))

    # Get vibes that are based on users interests
    #vibes_interests = [f['_id'] for f in db['vibes'].find({'_id': {'$nin': seen_vibes + vibes_followed_interests + vibes_followed}, 'activityType': {'$in': interests}})]
    vibes_interests = []

    for f in db['vibes'].find({'_id': {'$nin': seen_vibes + vibes_followed_interests}, 'activityType': {'$in': interests}}):
        vibes_interests.append(f['_id'])
        # TODO: Remove andy redundent data if found on redis
        r.lpush(REDIS_PREFIX + str(user['_id']) + ':recommended-medium', str(f['_id']))

    # # Get vibes that are not in interests
    #vibes_followed = [f['_id'] for f in db['vibes'].find({'_id': {'$nin': seen_vibes + vibes_followed_interests}, 'user': {'$in': following}})]
    vibes_followed = []

    for f in db['vibes'].find({'_id': {'$nin': seen_vibes + vibes_followed_interests + vibes_interests}, 'user': {'$in': following}}):
        vibes_followed.append(f['_id'])
        # TODO: Remove andy redundent data if found on redis
        r.lpush(REDIS_PREFIX + str(user['_id']) + ':recommended-medium', str(f['_id']))

    # Reserved vibes
    #other_vibes = [f['_id'] for f in db['vibes'].find({'_id': {'$nin': seen_vibes + vibes_followed_interests + vibes_followed + vibes_interests}})]

    for f in db['vibes'].find({'_id': {'$nin': seen_vibes + vibes_followed_interests + vibes_followed + vibes_interests}}):
        #r.lrem(str(user['_id']) + ':recommended-reserve', 0, str(f['_id']))
        r.lpush(REDIS_PREFIX + str(user['_id']) + ':recommended-reserve', str(f['_id']))
    

"""
    End of all precess
"""
finish = time.perf_counter()

print(f'Finished in {round(finish-start, 2)}')
print("Peak memory (MiB):",
      int(getrusage(RUSAGE_SELF).ru_maxrss / 1024))