#!/usr/bin/python3
"""
SQream backend module
According to example in https://pypi.org/project/pysqream/  
"""

import logging
logging.basicConfig(level=logging.DEBUG)

import pysqream

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

    return result

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

