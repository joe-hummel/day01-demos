#
# Inputs and executes queries against the MovieLens
# database running on the NU CS staff account in AWS.
#
# Prof. Joe Hummel
# Northwestern University
#

import datatier
from configparser import ConfigParser


#################################################################
#
# execute_select:
#
def execute_select(sql):
    #
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
    
    dbConn = datatier.get_dbConn(rds_endpoint, rds_portnum, rds_username, rds_pwd, rds_dbname)
      
    #
    # execute the SQL:
    #
    print(">>executing query...")
    rows = datatier.retrieve_all_rows(dbConn, sql)
    
    #
    # close connection and return result:
    #
    N = len(rows)
    
    if N==1:
      print(">>retrieved 1 row")
    else:
      print(">>retrieved", N, "rows")
    print()
    
    dbConn.close()
    return rows
    

#################################################################
#
# main
#
if __name__ == "__main__":
  #
  print("**Starting**")
  print()
  print("Enter a SELECT query to execute against MovieLens database>")
  print()
  
  sql = input()
  
  resultset = execute_select(sql)
  
  for row in resultset:
    print(row)
  
  print()
  print("**Done**")
