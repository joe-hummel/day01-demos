#
# Calls MovieLens web service hosted in AWS.
#
# Prof. Joe Hummel
# Northwestern University
#

import requests
from configparser import ConfigParser

print("**Starting**")
print()

#
# setup AWS based on config file:
#
print(">>reading config file...")

config_file = 'movielens-config.ini'    
configur = ConfigParser()
configur.read(config_file)

#
# get web service URL from config file:
#
endpoint = configur.get('webservice', 'endpoint')

#
# call the web service to retreive movies:
#
print(">>calling web service...")
print(endpoint)

print()
print("TODO")
print()

#
# TODO
#




print()
print("**Done**")
