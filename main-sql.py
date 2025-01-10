#
# Inputs and executes queries against the MovieLens
# database running on the NU CS staff account in AWS.
#
# Prof. Joe Hummel
# Northwestern University
#

import pymysql
from configparser import ConfigParser


print("**Starting**")
print()
#
# setup AWS based on config file:
#
config_file = 'movielens-config.ini'    
configur = ConfigParser()
configur.read(config_file)

#
# configure for RDS access
#
rds_endpoint = configur.get('rds', 'endpoint')
rds_portnum = int(configur.get('rds', 'port_number'))
rds_username = configur.get('rds', 'user_name')
rds_pwd = configur.get('rds', 'user_pwd')
rds_dbname = configur.get('rds', 'db_name')

#
# open connection to the database:
#
print(">>opening connection to MySQL server in AWS...")

dbConn = pymysql.connect(host=rds_endpoint,
                         port=rds_portnum,
                         user=rds_username,
                         passwd=rds_pwd,
                         database=rds_dbname)

#
# execute the SQL:
#
print(">>executing query...")

sql = """
      select * from movies limit 10 offset 0;
      """
#
# TODO
#
print()
print("TODO")
print()




print()
print("**Done**")
