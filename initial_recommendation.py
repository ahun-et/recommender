import os
import db
import time
import pymongo

from blender import bcolors
from resource import getrusage, RUSAGE_SELF

"""
    CONSTS
"""
REDIS_PREFIX = os.getenv('REDIS_PREFIX', 'usr:')

db.connect()

start = time.perf_counter()
"""
    Start all mesurable process
"""

finish = time.perf_counter()

i = 0

users_collection = db.mongo['users']
users = users_collection.find({})

for user in users:
    i = i + 1
    print(f'{bcolors.WARNING}USER: #{i}{bcolors.ENDC}')

    # User's interests
    interests = [f for f in user.get('interests', [])]

    # Get user's already seen vibes
    seen_vibes = [v['_id'] for v in db.mongo['vibeseens'].find({'userId': user['_id']})]
    # In order to avoid user's own vibe to user, treat users vibe as seen
    for f in db.mongo['vibes'].find({'user': user['_id']}):
        seen_vibes.append(f['_id'])

    # Get non blocked following
    following = [f['_id'] for f in db.mongo['useredges'].find({'source': user['_id'], 'request': 'FOLLOW'})]

    # Vibes of following users
    vibes_followed = []

    for f in db.mongo['vibes'].find({'_id': {'$nin': seen_vibes}, 'user': {'$in': following}}).sort('created_at', pymongo.DESCENDING):
        vibes_followed.append(f['_id'])
        db.redis.lpush(REDIS_PREFIX + str(user['_id']) + ':following', str(f['_id']))

    # Get vibes that are based on users interests    
    vibes_interests = []

    for f in db.mongo['vibes'].find({'_id': {'$nin': seen_vibes + vibes_followed}, 'activityType': {'$in': interests}}).sort('created_at', pymongo.DESCENDING):
        vibes_interests.append(f['_id'])
        # TODO: Remove andy redundent data if found on redis
        db.redis.lpush(REDIS_PREFIX + str(user['_id']) + ':suggested', str(f['_id']))

"""
    End of all precess
"""
finish = time.perf_counter()

print(f'Finished in {round(finish-start, 2)}')
print("Peak memory (MiB):",
      int(getrusage(RUSAGE_SELF).ru_maxrss / 1024))