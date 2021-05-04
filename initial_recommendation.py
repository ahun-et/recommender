import os

from blender import bcolors

try:
    print(f'{bcolors.OKBLUE}INFO: Connecting to the PostgreSQL databse... {bcolors.ENDC}')
except:
    except (psycopg2.DatabaseError, psycopg2.OperationalError) as ps_error:
            print(f'{bcolors.FAIL}ERROR: Failed to connect to PostgreSQL{bcolors.ENDC}')
            logger.exception(str(ps_error))