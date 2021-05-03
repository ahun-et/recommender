import os
import sys
import time
from dotenv import load_dotenv
from pymongo import MongoClient
from resource import getrusage, RUSAGE_SELF

load_dotenv()

"""
    Interests Score
"""
DEFAULT_INTEREST_WEIGHT = 0.1
FOLLOW_WEIGHT = 0.1
FOLLOWING_WEIGHT = 0.5

try:
    print('Connecting to the PostgreSQL databse...')
    mongodbUrl = os.getenv('MONGODB_URL')
    client = MongoClient(mongodbUrl)
    db = client.ahunbackup
except pymongo.errors.ServerSelectionTimeoutError as err:
    print(err)
    sys.exit(1)


start = time.perf_counter()
"""
    Start all mesurable process
"""

users_collection = db['users']
users = users_collection.find({})

for user in users:
    follow_weight = FOLLOW_WEIGHT
    total_weight = 0

    recommended_vibes = [] # contains follwing's not seen vibes

    # User's interests
    interests = [str(f['_id']) for f in user.get('interests', [])]
    
    # Get user's already seen vibes
    seen_vibes = [str(v['_id']) for v in db['vibeseens'].find({'userId': user['_id']})]

    # Get non blocked following
    following = [str(f['_id']) for f in db['useredges'].find({'source': user['_id'], 'request': 'FOLLOW'})]
    
    vibes = db['vibes'].find({'_id': {'$nin': seen_vibes}})
    
    # Loop through not seen vibes and calculate the user's interest
    # and push it to redis
    for vibe in vibes:
        if str(vibe['user']) in following:
            follow_weight = FOLLOWING_WEIGHT
            total_weight += FOLLOW_WEIGHT
        
        # Calculate vibe's activity type
        # TODO: calculate activityType if key not found using their business activityType
        for vibe_activity_type in vibe.get('activityType', []):
            if vibe_activity_type in interests:
                # Get the user's interest rating
                total_weight += DEFAULT_INTEREST_WEIGHT

        recommended_vibes.append(
            {
                'follow_weight': follow_weight,
                'vibe_id': str(vibe['_id'])
            }
        )

    #print(recommended_vibes)
    

"""
    End of all precess
"""
finish = time.perf_counter()

print(f'Finished in {round(finish-start, 2)}')
print("Peak memory (MiB):",
      int(getrusage(RUSAGE_SELF).ru_maxrss / 1024))