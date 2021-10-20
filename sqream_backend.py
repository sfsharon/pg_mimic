#!/usr/bin/python3
"""
SQream backend module
According to example in https://pypi.org/project/pysqream/  
"""

import logging
logging.basicConfig(level=logging.DEBUG)

import pysqream

def get_db(host, port, database, username, password) :
    logging.info("*** get_db : Connecting to SQream server {}:{}".format(HOST, PORT))
    con = pysqream.connect( host = HOST, port = PORT, 
                            database = DATABASE, 
                            username = USERNAME, password = PASSWORD)
    return con

def execute_query (connection, query) :
    cur = con.cursor()

    logging.info("*** Executing query {}".format(QUERY))
    cur.execute(QUERY)
    result = cur.fetchall()
    return result

if __name__ == "__main__" :
    HOST = "192.168.4.64"
    PORT = 5000
    DATABASE = "master"
    CLUSTERED = False
    USERNAME = "sqream"
    PASSWORD = "sqream"
    QUERY = "select * from test1"

    con = get_db(host = HOST, port = PORT, 
                 database = DATABASE, 
                 username = USERNAME, password = PASSWORD)

    res = execute_query(con, QUERY)

    print(res)

