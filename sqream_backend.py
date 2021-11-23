#!/usr/bin/python3
"""
SQream backend module
According to example in https://pypi.org/project/pysqream/  
"""

import logging
logging.basicConfig(level=logging.DEBUG)

import pysqream

# ***********************************************
# * Constants
# ***********************************************
BACKEND_QUERY__DESCRIPTION      = "backend_query__description"
BACKEND_QUERY__DESC_COLS_NAME   = "cols_name"
BACKEND_QUERY__DESC_COLS_TYPE   = "cols_type"
BACKEND_QUERY__DESC_COLS_LENGTH = "cols_length"
BACKEND_QUERY__DESC_COLS_FORMAT = "cols_format"


BACKEND_QUERY__RESULT       = "backend_query__result"

SQREAM_TYPE_INT             = 'ftInt'
SQREAM_COL_NOT_NULLABLE     = 'not null'

SQREAM_CATALOG_TABLES_QUERY  = "SELECT * FROM sqream_catalog.tables"
SQREAM_CATALOG_COLS_QUERY    = "SELECT GET_DDL('{tbl}')"
SQREAM_CATALOG_SCHEMA_NAME   = 'schema_name'
SQREAM_CATALOG_TABLE_NAME    = 'table_name'
SQREAM_CATALOG_COL_INFO_COL_NAME    = 'column_name'
SQREAM_CATALOG_COL_INFO_COL_TYPE    = 'column_type'
SQREAM_CATALOG_COL_INFO_IS_NULLABLE = 'column_is_nullable'
SQREAM_CATALOG_SCHEMA_INDEX  = 2
SQREAM_CATALOG_TABLE_INDEX   = 3


# Postgres Column formats 
COL_FORMAT_TEXT    = 0
COL_FORMAT_BINARY  = 1



# ***********************************************
# * Functionality
# ***********************************************

def get_db(host, port, database, username, password) :
    logging.info("get_db : Connecting to SQream server {}:{}".format(host, port))
    con = pysqream.connect( host, port,database, username, password)
    return con

def execute_query (connection, query) :
    """
    Execute a simple query on Sqream DB 
    """
    cur = connection.cursor()

    logging.info("Executing query: \"{}\"".format(query))
    cur.execute(query)

    # logging.debug("get_db : Column names {}".format(str(cur.col_names)))
    # logging.debug("get_db : Column types {}".format(str(cur.description)))

    result = cur.fetchall()

    # logging.debug("get_db : Result {}".format(str(result)))

    # Get column type
    cols_type   = [metadata[0] for metadata in cur.col_type_tups]
    cols_length = [metadata[1] for metadata in cur.col_type_tups]
    cols_name   = [metadata[0] for metadata in cur.description]
    
    num_of_cols = len(cols_type)
    cols_format = [COL_FORMAT_TEXT for i in range(num_of_cols)] # Hard coded - All columns are in Text format

    assert num_of_cols == len(cols_type) == len(cols_length) == len(cols_name), "Wrong number of column attributes"

    return {BACKEND_QUERY__DESCRIPTION : {BACKEND_QUERY__DESC_COLS_NAME   : cols_name,
                                          BACKEND_QUERY__DESC_COLS_TYPE   : cols_type,
                                          BACKEND_QUERY__DESC_COLS_LENGTH : cols_length,
                                          BACKEND_QUERY__DESC_COLS_FORMAT : cols_format},
            BACKEND_QUERY__RESULT      : result}

def sqream_catalog_tables(connection) :
    """
    Returns a list of all tables Schemas and names in current database 
    """

    res = execute_query(connection, SQREAM_CATALOG_TABLES_QUERY)

    table_details = []
    for table_detail in res[BACKEND_QUERY__RESULT] :
        table_details.append({SQREAM_CATALOG_SCHEMA_NAME : table_detail[SQREAM_CATALOG_SCHEMA_INDEX],
                              SQREAM_CATALOG_TABLE_NAME  : table_detail[SQREAM_CATALOG_TABLE_INDEX]})
    return table_details

def sqream_catalog_cols_info(table_name, connection) :
    """
    Returns information on the columns of a specific table (get_ddl) 
    """
    import re
    ERROR_MSG = f"Error in receiving column information for table {table_name}"
    CREATE_TABLE_LINE_REGEXP = r"create table \"(\w*)\"\.\"(\w*)\""
    COL_NAME_REGEXP = r"\W*\"\w*\""

    cols_details = []

    res = execute_query(connection, SQREAM_CATALOG_COLS_QUERY.format(tbl = table_name))

    assert len(res[BACKEND_QUERY__RESULT][0]) == 1, ERROR_MSG

    # Process DDL statement
    ddl_statement = res[BACKEND_QUERY__RESULT][0][0]
    ddl_lines = ddl_statement.split('\n')
    assert len(ddl_lines) > 0, ERROR_MSG
    received_table_name = re.findall(CREATE_TABLE_LINE_REGEXP, ddl_lines[0])[0][1]
    assert table_name == received_table_name, ERROR_MSG
    ddl_lines = ddl_lines[1:]
    for line in ddl_lines :
        line = line.strip()
        search_str = re.findall(COL_NAME_REGEXP, line)
        if len(search_str) > 0 :
            col_name = search_str[0]
            col_type_with_is_null = line[len(col_name):].strip()
            col_type = col_type_with_is_null.split()[0]
            col_is_nullable = "NO" if SQREAM_COL_NOT_NULLABLE in col_type_with_is_null else "YES" 
            col_name = col_name.strip("\"")
            cols_details.append({SQREAM_CATALOG_COL_INFO_COL_NAME    : col_name,
                                 SQREAM_CATALOG_COL_INFO_COL_TYPE    : col_type,
                                 SQREAM_CATALOG_COL_INFO_IS_NULLABLE : col_is_nullable})

    return cols_details


if __name__ == "__main__" :
    HOST = "192.168.4.64"
    PORT = 5000
    DATABASE = "master"
    CLUSTERED = False
    USERNAME = "sqream"
    PASSWORD = "sqream"
    QUERY = b"select * from test1"

    conn = get_db(host = HOST, port = PORT, 
                 database = DATABASE, 
                 username = USERNAME, password = PASSWORD)

    # res = execute_query(conn, QUERY)
    res = sqream_catalog_tables(conn)
    print(res)

