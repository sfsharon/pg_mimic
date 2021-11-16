#!/usr/bin/python3
"""
SQream backend module
According to example in https://pypi.org/project/pysqream/  
"""

import logging
logging.basicConfig(level=logging.DEBUG)

import pysqream

from pg_serdes import *

# ***********************************************
# * Constants
# ***********************************************
BACKEND_QUERY__DESCRIPTION   = "backend_query__description"
BACKEND_QUERY__RESULT        = "backend_query__result"

SQREAM_TYPE_INT              = 'ftInt'

# ***********************************************
# * Functionality
# ***********************************************

def get_db(host, port, database, username, password) :
    logging.info("*** get_db : Connecting to SQream server {}:{}".format(host, port))
    con = pysqream.connect( host, port,database, username, password)
    return con

def execute_query (connection, query) :
    cur = connection.cursor()

    logging.info("*** Executing query {}".format(query))
    cur.execute(query.decode("utf-8"))

    logging.info("*** get_db : Column names {}".format(str(cur.col_names)))
    logging.info("*** get_db : Column types {}".format(str(cur.description)))

    result = cur.fetchall()

    logging.info("*** get_db : Result {}".format(str(result)))

    # Get column type
    cols_type   = [metadata[0] for metadata in cur.col_type_tups]
    cols_length = [metadata[1] for metadata in cur.col_type_tups]
    cols_name   = [metadata[0] for metadata in cur.description]
    
    num_of_cols = len(cols_type)

    cols_format = [COL_FORMAT_TEXT for i in range(num_of_cols)] # Hard coded - All columns are in Text format

    assert num_of_cols == len(cols_type) == len(cols_length) == len(cols_name), "Wrong number of column attributes"

    cols_desc = prepare_cols_desc(cols_name, cols_type, cols_length, cols_format)

    
    return {BACKEND_QUERY__DESCRIPTION : cols_desc,
            BACKEND_QUERY__RESULT      : result}

if __name__ == "__main__" :
    HOST = "192.168.4.64"
    PORT = 5000
    DATABASE = "master"
    CLUSTERED = False
    USERNAME = "sqream"
    PASSWORD = "sqream"
    QUERY = b"select * from test1"

    con = get_db(host = HOST, port = PORT, 
                 database = DATABASE, 
                 username = USERNAME, password = PASSWORD)

    res = execute_query(con, QUERY)

    print(res)

