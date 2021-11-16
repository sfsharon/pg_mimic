#!/usr/bin/python3
"""
Postgres Serialize-Deserialize messages module.
Postgres data formats : https://www.postgresql.org/docs/12/protocol-message-formats.html          
"""

import logging
logging.basicConfig(level=logging.DEBUG)

import struct

# ***********************************************
# * Constants
# ***********************************************
# Deserialize Message IDs 
STARTUP_MSG_ID = bytes('STARTUP', "utf-8")
QUERY_MSG_ID = bytes('Q', "utf-8")
PARSE_MSG_ID = bytes('P', "utf-8")
PASSWORD_MSG_ID = bytes('p', "utf-8")
BIND_MSG_ID = bytes('B', "utf-8")
DESCRIBE_MSG_ID = bytes('D', "utf-8")
EXECUTE_MSG_ID = bytes('E', "utf-8")
SYNC_MSG_ID = bytes('S', "utf-8")
# Serialize Message IDs 
PARAMETER_STATUS_MSG_ID = bytes('S', "utf-8")
AUTHENTICATION_REQUEST_MSG_ID = bytes('R', "utf-8")
READY_FOR_QUERY_MSG_ID = bytes('Z', "utf-8")
CMD_COMPLETE_MSG_ID = bytes('C', "utf-8")
DATA_COLS_MSG_ID = bytes('D', "utf-8")
ROW_DESC_MSG_ID = bytes('T', "utf-8")
PARSE_COMPLETE_MSG_ID = bytes('1', "utf-8")
BIND_COMPLETE_MSG_ID = bytes('2', "utf-8")

# Server state
READY_FOR_QUERY_SERVER_STATUS_IDLE = bytes('I', "utf-8")

# Message attributes
MSG_ID = "msg_id"

QUERY_MSG__SIMPLE_QUERY = "simple_query"

PARSE_MSG__STATEMENT = "statement"
PARSE_MSG__QUERY     = "query"
PARSE_MSG__PARAMETER = "parameter"

BIND_MSG__STATEMENT         = "statement"
BIND_MSG__PORTAL            = "portal"
BIND_MSG__PARAM_FORMATS     = "param_formats"
BIND_MSG__PARAM_VALUES      = "param_values"
BIND_MSG__RESULT_FORMATS    = "result_formats"
BIND_MSG__FORMAT_TYPES      = "format_type"

DESCRIBE_MSG__PARAM_FORMATS = "description"
DESCRIBE_MSG__PORTAL        = "portal"
    
EXECUTE_MSG__PORTAL         = "portal"
EXECUTE_MSG__ROWS_TO_RETURN = "rows_to_return"

# Column description (T message)
# ------------------------------
# Description dictionary names
COL_DESC__NAME   = "col_desc_name"
COL_DESC__TYPE   = "col_desc_type"
COL_DESC__FORMAT = "col_desc_format"
COL_DESC__LENGTH = "col_desc_length"

# Postgres Column formats 
COL_FORMAT_TEXT    = 0
COL_FORMAT_BINARY  = 1

# Postgres Column types 
COL_INT_TYPE_OID = 23
COL_LONG_INT_TYPE_OID = 26
COL_TEXT_TYPE_OID = 19
COL_CHAR_TYPE_OID = 18

# Misc
NULL_TERMINATOR = b'\x00'
PBI_CATALOG_SUPPORTED_TYPES_QUERY           = b"\r\n/*** Load all supported types ***/\r\nSELECT ns.nspname, a.typname, a.oid, a.typrelid, a.typbasetype,\r\nCASE WHEN pg_proc.proname='array_recv' THEN 'a' ELSE a.typtype END AS type,\r\nCASE\r\n  WHEN pg_proc.proname='array_recv' THEN a.typelem\r\n  WHEN a.typtype='r' THEN rngsubtype\r\n  ELSE 0\r\nEND AS elemoid,\r\nCASE\r\n  WHEN pg_proc.proname IN ('array_recv','oidvectorrecv') THEN 3    /* Arrays last */\r\n  WHEN a.typtype='r' THEN 2                                        /* Ranges before */\r\n  WHEN a.typtype='d' THEN 1                                        /* Domains before */\r\n  ELSE 0                                                           /* Base types first */\r\nEND AS ord\r\nFROM pg_type AS a\r\nJOIN pg_namespace AS ns ON (ns.oid = a.typnamespace)\r\nJOIN pg_proc ON pg_proc.oid = a.typreceive\r\nLEFT OUTER JOIN pg_class AS cls ON (cls.oid = a.typrelid)\r\nLEFT OUTER JOIN pg_type AS b ON (b.oid = a.typelem)\r\nLEFT OUTER JOIN pg_class AS elemcls ON (elemcls.oid = b.typrelid)\r\nLEFT OUTER JOIN pg_range ON (pg_range.rngtypid = a.oid) \r\nWHERE\r\n  a.typtype IN ('b', 'r', 'e', 'd') OR         /* Base, range, enum, domain */\r\n  (a.typtype = 'c' AND cls.relkind='c') OR /* User-defined free-standing composites (not table composites) by default */\r\n  (pg_proc.proname='array_recv' AND (\r\n    b.typtype IN ('b', 'r', 'e', 'd') OR       /* Array of base, range, enum, domain */\r\n    (b.typtype = 'p' AND b.typname IN ('record', 'void')) OR /* Arrays of special supported pseudo-types */\r\n    (b.typtype = 'c' AND elemcls.relkind='c')  /* Array of user-defined free-standing composites (not table composites) */\r\n  )) OR\r\n  (a.typtype = 'p' AND a.typname IN ('record', 'void'))  /* Some special supported pseudo-types */\r\nORDER BY ord\x00"
PBI_CATALOG_FIELD_DEF_COMPOSITE_TYPES_QUERY = b"/*** Load field definitions for (free-standing) composite types ***/\r\nSELECT typ.oid, att.attname, att.atttypid\r\nFROM pg_type AS typ\r\nJOIN pg_namespace AS ns ON (ns.oid = typ.typnamespace)\r\nJOIN pg_class AS cls ON (cls.oid = typ.typrelid)\r\nJOIN pg_attribute AS att ON (att.attrelid = typ.typrelid)\r\nWHERE\r\n  (typ.typtype = 'c' AND cls.relkind='c') AND\r\n  attnum > 0 AND     /* Don't load system attributes */\r\n  NOT attisdropped\r\nORDER BY typ.oid, att.attnum\x00"
PBI_CATALOG_ENUM_FIELDS_QUERY               = b'/*** Load enum fields ***/\r\nSELECT pg_type.oid, enumlabel\r\nFROM pg_enum\r\nJOIN pg_type ON pg_type.oid=enumtypid\r\nORDER BY oid, enumsortorder\x00'

# ***********************************************
# * Utility functions
# ***********************************************
def prepare_cols_desc(cols_name, cols_type, cols_length, cols_format):
    """! Prepare the columns description object, needed by the T message
    @param cols_name
    @param cols_type
    @param cols_length
    @param cols_format

    @return cols_desc
    """
    from sqream_backend import SQREAM_TYPE_INT 

    num_of_cols = len(cols_name)

    assert num_of_cols == len(cols_type) == len(cols_length) == len(cols_format), "Mismatch in number columns description attributes"

    cols_desc = []
    for index in range(num_of_cols) :
        # Translate SQream type to Postgres type
        if cols_type[index] == SQREAM_TYPE_INT :
            cols_type[index] = COL_INT_TYPE_OID
        
        cols_desc.append({COL_DESC__NAME   : cols_name[index],
                          COL_DESC__TYPE   : cols_type[index],
                          COL_DESC__FORMAT : cols_format[index],
                          COL_DESC__LENGTH : cols_length[index]})

    return cols_desc

def utility_int_to_text(val) :
    """! Translate a string to an ordinal string, little endian
    @param val integer to translate

    @return bytes comprises of list of ordinals ascii value, representing the input val
            For example : int value 192, return value 0x31/0x39/0x32
    """
    rVal = bytes('', "utf-8")
    while val != 0 :
        digit = val % 10
        val = val // 10
        char_digit = struct.pack("!c",  bytes(str(digit), "utf-8"))
        rVal += char_digit
    rVal = rVal[::-1]           # Reverse the string
    return rVal

def is_password_msg(msg):
    """
    Verify message is a PASSWORD message
    """

    return True if msg[MSG_ID] == PASSWORD_MSG_ID else False

def is_pg_catalog_msg(query):
    """!  Identify PowerBI Postgres catalog messages
    @param query: Input string query

    @return Boolean: True if catalog message, False otherwise.
    """
    is_pg_catalog = False

    if query == PBI_CATALOG_SUPPORTED_TYPES_QUERY :
        logging.info("*** Received PG Catalog Supported types query")
        is_pg_catalog = True
    elif query == PBI_CATALOG_FIELD_DEF_COMPOSITE_TYPES_QUERY :
        logging.info("*** Received PG Catalog field definition composite types query")
        is_pg_catalog = True
    elif query == PBI_CATALOG_ENUM_FIELDS_QUERY :
        logging.info("*** Received PG Catalog enum fields query")
        is_pg_catalog = True

    return is_pg_catalog

def prepare_pg_catalog_cols_desc(query):
    """! Prepare column description to a PG catalog query
    """

    cols_desc = [{}, {}, {}, {}]

    if query == PBI_CATALOG_SUPPORTED_TYPES_QUERY :
        cols_name   = ['nspname',                 'typname',                'oid',                   'typrelid',                     'typbasetype',                    'type',                'elemoid',                    'ord']        
        cols_type   = [COL_TEXT_TYPE_OID,         COL_TEXT_TYPE_OID,        COL_LONG_INT_TYPE_OID,    COL_LONG_INT_TYPE_OID,         COL_LONG_INT_TYPE_OID,            COL_CHAR_TYPE_OID,     COL_LONG_INT_TYPE_OID,        COL_INT_TYPE_OID]
        cols_length = [64,                        64,                       4,                        4,                             4,                                1,                      4,                           4]
        cols_format = [COL_FORMAT_TEXT,           COL_FORMAT_TEXT,          COL_FORMAT_TEXT,          COL_FORMAT_TEXT,               COL_FORMAT_TEXT,                  COL_FORMAT_TEXT,        COL_FORMAT_TEXT,             COL_FORMAT_TEXT]
    elif query == PBI_CATALOG_FIELD_DEF_COMPOSITE_TYPES_QUERY :
        cols_name   = ['oid',                     'attname',                 'atttypid']
        cols_type   = [COL_LONG_INT_TYPE_OID,      COL_TEXT_TYPE_OID,        COL_LONG_INT_TYPE_OID]
        cols_length = [4,                          64,                       4]
        cols_format = [COL_FORMAT_TEXT,           COL_FORMAT_TEXT,          COL_FORMAT_TEXT]
    elif query == PBI_CATALOG_ENUM_FIELDS_QUERY :
        cols_name   = ['oid',                     'enumlabel']
        cols_type   = [COL_LONG_INT_TYPE_OID,      COL_TEXT_TYPE_OID]
        cols_length = [4,                          64]
        cols_format = [COL_FORMAT_TEXT,           COL_FORMAT_TEXT]
    else :
        raise ValueError('Received unknown pg catalog query ')

    cols_desc = prepare_cols_desc(cols_name, cols_type, cols_length, cols_format)
    return cols_desc

def prepare_pg_catalog_cols_value(query) :
    """! Prepare PG Catalog column values to a PG catalog query
    """
    empty_cols_values = []

    if query == PBI_CATALOG_SUPPORTED_TYPES_QUERY :
        cols_values = [['pg_catalog', 'float8', 701, 0, 0, 'b', 0, 0],
                       ['pg_catalog', 'tid'   , 27,  0, 0, 'b', 0, 0]]
        return cols_values 
    elif query == PBI_CATALOG_FIELD_DEF_COMPOSITE_TYPES_QUERY :
        return empty_cols_values
    elif query == PBI_CATALOG_ENUM_FIELDS_QUERY :
        return empty_cols_values
    else :
        raise ValueError('Received unknown pg catalog query ')


def tokenization(data, is_expecting_startup_msg):
    """
    Tokenize a stream of bytes into a list of tuples with two values :
        [(Header Msg ID size of byte, Msg payload), 
        (Msg ID_2, Payload_2), ...].
    This is a first step in parsing the incoming PG message.
    Next step would be to build a sentence with a specific meaning out of a series of words
    """
    tokenized_msgs = []

    while len(data) > 0 :
        # Get message at the start of the data frame - MsgID / Length
        if is_expecting_startup_msg == True :
            HEADERFORMAT = "!i"     
            header_len = struct.calcsize(HEADERFORMAT)
            msg_len = struct.unpack(HEADERFORMAT, data[0:header_len])[0]
            msg_id = ''
        else : 
            HEADERFORMAT = "!ci"     
            header_len = struct.calcsize(HEADERFORMAT)
            msg_id, msg_len = struct.unpack(HEADERFORMAT, data[0:header_len])

        msg_data = data[header_len:msg_len + 1]
        tokenized_msgs.append((msg_id, msg_data))

        # Iterate to the next message
        data = data[msg_len + 1 :]

    return tokenized_msgs

def parse(tokenized_msgs, is_expecting_startup_msg) :
    """
    Parse tokenized messages into Postgres messages
    """
    parsed_msgs = []

    # Special case for Startup message, which do not have Message ID character at the message beginning
    for msg in tokenized_msgs :
        msg_id = msg[0]
        if is_expecting_startup_msg == True :
            parsed_msgs.append(Startup_Msg_Deserialize(msg))
        elif msg_id == QUERY_MSG_ID :
            parsed_msgs.append(Q_Msg_Simple_Query_Deserialize(msg))
        elif msg_id == PASSWORD_MSG_ID :
            parsed_msgs.append({MSG_ID : PASSWORD_MSG_ID})
        elif msg_id == PARSE_MSG_ID :
            parsed_msgs.append(P_Msg_Parse_Deserialize(msg))
        elif msg_id == BIND_MSG_ID :
            parsed_msgs.append(B_Msg_Bind_Deserialize(msg))
        elif msg_id == DESCRIBE_MSG_ID :
            parsed_msgs.append(D_Msg_Describe_Deserialize(msg))
        elif msg_id == EXECUTE_MSG_ID :
            parsed_msgs.append(E_Msg_Execute_Deserialize(msg))
        elif msg_id == SYNC_MSG_ID :
            parsed_msgs.append( S_Msg_Sync_Deserialize(msg))
        else :
            assert 0, f"Unrecognized message id : {msg_id}"
            
    return parsed_msgs


# ***********************************************
# * Serialize / Deserialize functions
# ***********************************************

def Startup_Msg_Deserialize(data) :
    """! Deserialize startup message
    @param N/A

    @return N/A

    StartupMessage (Frontend)
        Int32(196608)
        The protocol version number. The most significant 16 bits are the major version number (3 for the protocol described here). The least significant 16 bits are the minor version number (0 for the protocol described here).

        The protocol version number is followed by one or more pairs of parameter name and value strings. A zero byte is required as a terminator after the last name/value pair. Parameters can appear in any order. user is required, others are optional. Each parameter is specified as:

        String
        The parameter name. Currently recognized names are:

        user
        The database user name to connect as. Required; there is no default.

        database
        The database to connect to. Defaults to the user name.

        options
        Command-line arguments for the backend. (This is deprecated in favor of setting individual run-time parameters.) Spaces within this string are considered to separate arguments, unless escaped with a backslash (\); write \\ to represent a literal backslash.

        replication
        Used to connect in streaming replication mode, where a small set of replication commands can be issued instead of SQL statements. Value can be true, false, or database, and the default is false. See Section 52.4 for details.

        In addition to the above, other parameters may be listed. Parameter names beginning with _pq_. are reserved for use as protocol extensions, while others are treated as run-time parameters to be set at backend start time. Such settings will be applied during backend start (after parsing the command-line arguments if any) and will act as session defaults.

        String
        The parameter value.        
    """
    payload = data[1]

    parsed_msg = {}

    parsed_msg[MSG_ID] = STARTUP_MSG_ID 

    PAYLOAD_STRUCT = "!hh"     
    protocol_major_ver, protocol_minor_ver = struct.unpack(PAYLOAD_STRUCT, payload[0:struct.calcsize(PAYLOAD_STRUCT)])

    logging.info("Startup message : protocol major: %d, protocol minor: %d", protocol_major_ver, protocol_minor_ver)

    return parsed_msg

def Q_Msg_Simple_Query_Deserialize(data) :
    """! Deserialize simple query message
    @param data bytes array of the simple query

    @return query string

    Query (Frontend)
        String
        The query string itself.        
    """

    msg_id = data[0]
    payload = data[1]

    parsed_msg = {}

    assert msg_id == QUERY_MSG_ID, f"Received '{msg_id}' unexpected message ID"

    parsed_msg[MSG_ID] = msg_id

    simple_query = payload[ : payload.find(NULL_TERMINATOR) + 1]
    parsed_msg[QUERY_MSG__SIMPLE_QUERY] = simple_query

    logging.info("*** Q_Msg_Simple_Query_Deserialize: Query received \"{}\"".format(simple_query))

    return parsed_msg

def P_Msg_Parse_Deserialize(data) :
    """! Deserialize Parse message
    @param data bytes array of the simple query

    @return Parse 

    Parse (Frontend)

    String
    The name of the destination prepared statement (an empty string selects the unnamed prepared statement).

    String
    The query string to be parsed.

    Int16
    The number of parameter data types specified (can be zero). Note that this is not an indication of the number of parameters that might appear in the query string, only the number that the frontend wants to prespecify types for.

    Then, for each parameter, there is the following:

    Int32
    Specifies the object ID of the parameter data type. Placing a zero here is equivalent to leaving the type unspecified.       
    """
    msg_id = data[0]
    payload = data[1]

    parsed_msg = {}

    assert msg_id == PARSE_MSG_ID, f"Received '{msg_id}' unexpected message ID"

    parsed_msg[MSG_ID] = msg_id

    statement = payload[ : payload.find(NULL_TERMINATOR) + 1]
    parsed_msg[PARSE_MSG__STATEMENT] = statement
    payload = payload[len(statement) : ]

    query = payload[ : payload.find(NULL_TERMINATOR) + 1]
    parsed_msg[PARSE_MSG__QUERY] = query
    payload = payload[len(query) : ]

    PAYLOAD_STRUCT = "!h"     
    parameter = struct.unpack(PAYLOAD_STRUCT, payload[0:struct.calcsize(PAYLOAD_STRUCT)])
    parsed_msg[PARSE_MSG__PARAMETER] = parameter[0]

    return parsed_msg

def B_Msg_Bind_Deserialize(data) :
    """! Deserialize Bind message
    @param data bytes array 

    @return  

    Bind (Frontend)
        String
        The name of the destination portal (an empty string selects the unnamed portal).

        String
        The name of the source prepared statement (an empty string selects the unnamed prepared statement).

        Int16
        The number of parameter format codes that follow (denoted C below). This can be zero to indicate that there are 
        no parameters or that the parameters all use the default format (text); 
        or one, in which case the specified format code is applied to all parameters; 
        or it can equal the actual number of parameters.

        Int16[C]
        The parameter format codes. Each must presently be zero (text) or one (binary).

        Int16
        The number of parameter values that follow (possibly zero). This must match the number of parameters needed by the query.

        Next, the following pair of fields appear for each parameter:

        Int32
        The length of the parameter value, in bytes (this count does not include itself). Can be zero. As a special case, 
        -1 indicates a NULL parameter value. No value bytes follow in the NULL case.

        Byten
        The value of the parameter, in the format indicated by the associated format code. n is the above length.

        After the last parameter, the following fields appear:

        Int16
        The number of result-column format codes that follow (denoted R below). This can be zero to indicate that there 
        are no result columns or that the result columns should all use the default format (text); 
        or one, in which case the specified format code is applied to all result columns (if any); or it can equal the actual number of result columns of the query.

        Int16[R]
        The result-column format codes. Each must presently be zero (text) or one (binary). 
    """
    msg_id = data[0]
    payload = data[1]

    parsed_msg = {}

    assert msg_id == BIND_MSG_ID, f"Received '{msg_id}' unexpected message ID"

    parsed_msg[MSG_ID] = msg_id

    portal = payload[ : payload.find(NULL_TERMINATOR) + 1]
    parsed_msg[BIND_MSG__PORTAL] = portal
    payload = payload[len(portal) : ]

    statement = payload[ : payload.find(NULL_TERMINATOR) + 1]
    parsed_msg[BIND_MSG__STATEMENT] = statement
    payload = payload[len(statement) : ]

    PAYLOAD_STRUCT = "!hhhh"     
    param_formats, param_values, result_formats, format_type = struct.unpack(PAYLOAD_STRUCT, payload[0:struct.calcsize(PAYLOAD_STRUCT)])
    parsed_msg[BIND_MSG__PARAM_FORMATS]  = param_formats
    parsed_msg[BIND_MSG__PARAM_VALUES]   = param_values
    parsed_msg[BIND_MSG__RESULT_FORMATS] = result_formats
    parsed_msg[BIND_MSG__FORMAT_TYPES]   = format_type

    return parsed_msg


def D_Msg_Describe_Deserialize(data) :
    """! Deserialize Describe message
    @param data bytes array 

    @return  

    Describe (Frontend)
        Byte1
        'S' to describe a prepared statement; or 'P' to describe a portal.

        String
        The name of the prepared statement or portal to describe (an empty string selects the unnamed prepared statement or portal).


    """
    msg_id = data[0]
    payload = data[1]

    parsed_msg = {}

    assert msg_id == DESCRIBE_MSG_ID, f"Received '{msg_id}' unexpected message ID"

    parsed_msg[MSG_ID] = msg_id

    PAYLOAD_STRUCT = "!c"     
    description = struct.unpack(PAYLOAD_STRUCT, payload[0:struct.calcsize(PAYLOAD_STRUCT)])
    parsed_msg[DESCRIBE_MSG__PARAM_FORMATS]  = description
    payload = payload[struct.calcsize(PAYLOAD_STRUCT) : ]

    portal = payload[ : payload.find(NULL_TERMINATOR) + 1]
    parsed_msg[DESCRIBE_MSG__PORTAL] = portal

    return parsed_msg

def E_Msg_Execute_Deserialize(data) :
    """! Deserialize Execute message
    @param data bytes array 

    @return  

    Execute (Frontend)
        String
        The name of the portal to execute (an empty string selects the unnamed portal).

        Int32
        Maximum number of rows to return, if portal contains a query that returns rows (ignored otherwise). Zero denotes “no limit”.

    """
    msg_id = data[0]
    payload = data[1]

    parsed_msg = {}

    assert msg_id == EXECUTE_MSG_ID, f"Received '{msg_id}' unexpected message ID"

    parsed_msg[MSG_ID] = msg_id

    portal = payload[ : payload.find(NULL_TERMINATOR) + 1]
    parsed_msg[EXECUTE_MSG__PORTAL] = portal
    payload = payload[len(portal) : ]

    PAYLOAD_STRUCT = "!i"     
    rows_to_return = struct.unpack(PAYLOAD_STRUCT, payload[0:struct.calcsize(PAYLOAD_STRUCT)])
    parsed_msg[EXECUTE_MSG__ROWS_TO_RETURN]  = rows_to_return

    return parsed_msg

def S_Msg_Sync_Deserialize(data) :
    """! Deserialize Execute message
    @param data bytes array 

    @return  

    Sync (Frontend)

    """
    msg_id = data[0]
    payload = data[1]

    parsed_msg = {}

    assert msg_id == SYNC_MSG_ID, f"Received '{msg_id}' unexpected message ID"

    parsed_msg[MSG_ID] = msg_id

    return parsed_msg


def S_Msg_ParameterStatus_Serialize(param_name, param_value) :
    """! Serialize a parameter status.
    @param param_name   string
    @param param_value  string

    @return packed bytes of parameter status (S message)         

    ParameterStatus (Backend)
        Byte1('S')
        Identifies the message as a run-time parameter status report.

        Int32
        Length of message contents in bytes, including self.

        String
        The name of the run-time parameter being reported.

        String
        The current value of the parameter.
    """
    HEADERFORMAT = "!i"         # Length 

    Length = struct.calcsize(HEADERFORMAT) +    \
                len(param_name) + 1 +              \
                len(param_value) + 1 

    rVal = PARAMETER_STATUS_MSG_ID + struct.pack(HEADERFORMAT, Length) + \
            param_name  + NULL_TERMINATOR +                       \
            param_value + NULL_TERMINATOR

    return rVal

def D_Msg_DataRow_Serialize(cols_desc, cols_values) :
    """! Serialize a data col section.
    @param cols_desc description of columns - Column name, type, format and length
    @param cols_values list of column values

    @return packed bytes of column values (D message)         

    DataRow (Backend)
        Byte1('D') (MSG_ID)
        Identifies the message as a data row.

        Int32 (Length)
        Length of message contents in bytes, including self.

        Int16 (Field_count)
        The number of column values that follow (possibly zero).

        Next, the following pair of fields appear for each column:

        Int32 (Column_length)
        The length of the column value, in bytes (this count does not include itself). Can be zero. As a special case, -1 indicates a NULL column value. No value bytes follow in the NULL case.

        Byten (Data)
        The value of the column, in the format indicated by the associated format code. n is the above length.
    """

    HEADERFORMAT = "!ih"        # Length / Field count
    COLDESC_FORMAT = "!i"       # Column length

    msg = bytes('', "utf-8")

    assert len(cols_values) == len(cols_desc), "Number of columns values and number of columns types do not match"

    fields_count = len(cols_values)

    for index, col_value in enumerate (cols_values) :
        if cols_desc[index][COL_DESC__FORMAT] == COL_FORMAT_BINARY or \
           cols_desc[index][COL_DESC__TYPE] == COL_TEXT_TYPE_OID or \
           cols_desc[index][COL_DESC__TYPE] == COL_CHAR_TYPE_OID :
                col_value_string = bytes(col_value, "utf-8")
        elif cols_desc[index][COL_DESC__FORMAT] == COL_FORMAT_TEXT :
                col_value_string = utility_int_to_text(col_value)
        else :
            raise ValueError('Unsupported serialize type : ', cols_desc[index])

        msg += struct.pack(COLDESC_FORMAT, len(col_value_string)) + col_value_string

    Length = struct.calcsize(HEADERFORMAT) + len(msg)

    msg = DATA_COLS_MSG_ID + struct.pack(HEADERFORMAT, Length, fields_count) + msg

    return msg


def T_Msg_RowDescription_Serialize(cols_desc):
        """! Serialize a row description section.

        @param cols_desc: list of column attributes (e.g. name, type)

        @return packed bytes of rows description (T message)        

        RowDescription (Backend)

            Byte1('T') (MSG_ID)
            Identifies the message as a row description.

            Int32 (Length)
            Length of message contents in bytes, including self.

            Int16
            Specifies the number of fields in a row (can be zero).

            Then, for each field, there is the following:

            String (row_name)
            The field name.

            Int32 (Table_OID)
            If the field can be identified as a column of a specific table, the object ID of the table; otherwise zero.

            Int16 (Column_index)
            If the field can be identified as a column of a specific table, the attribute number of the column; otherwise zero.

            Int32 (Type_OID)
            The object ID of the field's data type.

            Int16 (Column_length)
            The data type size (see pg_type.typlen). Note that negative values denote variable-width types.

            Int32 (Type_modifier)
            The type modifier (see pg_attribute.atttypmod). The meaning of the modifier is type-specific.

            Int16 (Format)
            The format code being used for the field. Currently will be zero (text) or one (binary). 
            In a RowDescription returned from the statement variant of Describe, the format code is not yet known and will always be zero.
        """
        # MSG_ID = 'T'                    # Type
        HEADERFORMAT = "!ih"            # Length / Field count
        ROWDESC_FORMAT = "!ihihih"      # Table OID / Column index / Type OID / Column length / Type modifier / Format

        msg = bytes('', "utf-8")

        field_count = len(cols_desc) # len(row_names)

        for count, col_desc in enumerate (cols_desc) :            
            null_term_row_name = bytes(col_desc[COL_DESC__NAME], "utf-8") + NULL_TERMINATOR
            
            Table_OID = 49152                          # Hard coded value
            Column_index = count + 1
            Type_OID = col_desc[COL_DESC__TYPE]        # More information on OID Types : https://www.postgresql.org/docs/9.4/datatype-oid.html
            Column_length = col_desc[COL_DESC__LENGTH] 
            Type_modifier = -1                         # Hard coded value. 
            Format = col_desc[COL_DESC__FORMAT] 
            msg += null_term_row_name + struct.pack(ROWDESC_FORMAT, 
                                                    Table_OID, 
                                                    Column_index, 
                                                    Type_OID, 
                                                    Column_length, 
                                                    Type_modifier, 
                                                    Format)

        Length = struct.calcsize(HEADERFORMAT) + len(msg)

        msg = ROW_DESC_MSG_ID + struct.pack(HEADERFORMAT, Length, field_count) + msg

        return msg

def C_Msg_CommandComplete_Serialize(tag_name) :
    """! Serialize a command complete section.
    @param tag_name Null terminated string of the tag name

    @return packed bytes of command complete (C message)         

    CommandComplete (Backend)
        Byte1('C')
        Identifies the message as a command-completed response.

        Int32
        Length of message contents in bytes, including self.

        String
        The command tag. This is usually a single word that identifies which SQL command was completed.

        For an INSERT command, the tag is INSERT oid rows, where rows is the number of rows inserted. oid used to be the 
                               object ID of the inserted row if rows was 1 and the target table had OIDs, 
                               but OIDs system columns are not supported anymore; therefore oid is always 0.

        For a DELETE command, the tag is DELETE rows where rows is the number of rows deleted.

        For an UPDATE command, the tag is UPDATE rows where rows is the number of rows updated.

        For a SELECT or CREATE TABLE AS command, the tag is SELECT rows where rows is the number of rows retrieved.

        For a MOVE command, the tag is MOVE rows where rows is the number of rows the cursor's position has been changed by.

        For a FETCH command, the tag is FETCH rows where rows is the number of rows that have been retrieved from the cursor.

        For a COPY command, the tag is COPY rows where rows is the number of rows copied. (Note: the row count appears only in PostgreSQL 8.2 and later.)

    """
    HEADERFORMAT = "!i"         # Length 

    tag_name_null_term = bytes(tag_name , "utf-8") +  NULL_TERMINATOR

    Length = struct.calcsize(HEADERFORMAT) + len(tag_name_null_term)        

    msg = CMD_COMPLETE_MSG_ID + struct.pack(HEADERFORMAT, Length) + tag_name_null_term

    return msg

def R_Msg_AuthRequest_Serialize():
    """
    AuthenticationMD5Password (Backend)
        Byte1('R')
        Identifies the message as an authentication request.

        Int32(12)
        Length of message contents in bytes, including self.

        Int32(5)
        Specifies that an MD5-encrypted password is required.

        Byte4
        The salt to use when encrypting the password.
    """

    PAYLOAD_FORMAT = "!iii"    
    
    Length = struct.calcsize(PAYLOAD_FORMAT)

    auth_req_OK = AUTHENTICATION_REQUEST_MSG_ID + \
                  struct.pack(PAYLOAD_FORMAT, Length, 5, 0x12345678) # Authentication MD5 

    return auth_req_OK

def R_Msg_AuthOk_Serialize():
    """
    AuthenticationOk (B)
    Byte1('R')
    Identifies the message as an authentication request.

    Int32(8)
    Length of message contents in bytes, including self.

    Int32(0)
    Specifies that the authentication was successful.
    """
    PAYLOAD_FORMAT = "!ii"    
    
    Length = struct.calcsize(PAYLOAD_FORMAT)

    auth_req_OK = AUTHENTICATION_REQUEST_MSG_ID + \
                  struct.pack(PAYLOAD_FORMAT, Length, 0) # Authentication Request OK

    return auth_req_OK

def Z_Msg_ReadyForQuery_Serialize(server_status) :
    """! Serialize a ready for query section.
    @param server_status - Enumeration for the server status ('T' / 'T' / 'E')

    @return packed bytes of ready for query (Z message)         

    ReadyForQuery (Backend)
        Byte1('Z')
        Identifies the message type. ReadyForQuery is sent whenever the backend is ready for a new query cycle.

        Int32(5)
        Length of message contents in bytes, including self.

        Byte1
        Current backend transaction status indicator. Possible values are :
        'I' if idle (not in a transaction block); 
        'T' if in a transaction block; 
        'E' if in a failed transaction block (queries will be rejected until block is ended).

    """
    HEADERFORMAT = "!i"         # Length 

    Length = struct.calcsize(HEADERFORMAT) + len(server_status)

    msg = READY_FOR_QUERY_MSG_ID + struct.pack(HEADERFORMAT, Length) + READY_FOR_QUERY_SERVER_STATUS_IDLE

    return msg


def One_Msg_ParseComplete_Serialize() :
    """! Serialize a parse complete section.
    @param 

    @return

    ParseComplete (Backend)
        Byte1('1')
        Identifies the message as a Parse-complete indicator.

        Int32(4)
        Length of message contents in bytes, including self.

    """
    HEADERFORMAT = "!i"         # Length 

    Length = struct.calcsize(HEADERFORMAT) 

    msg = PARSE_COMPLETE_MSG_ID + struct.pack(HEADERFORMAT, Length) 

    return msg


def Two_Msg_BindComplete_Serialize() :
    """! Serialize a bind complete section.
    @param 

    @return

    BindComplete (Backend)
        Byte1('2')
        Identifies the message as a Bind-complete indicator.

        Int32(4)
        Length of message contents in bytes, including self.

    """
    HEADERFORMAT = "!i"         # Length 

    Length = struct.calcsize(HEADERFORMAT) 

    msg = BIND_COMPLETE_MSG_ID + struct.pack(HEADERFORMAT, Length) 

    return msg

# *****************************************************
# * Unit Testing
# *****************************************************
def PBI_UT() :
    """
    Power BI Unit testing
    """
    # Example input
    PBDES_Msg = b'P\x00\x00\x00H\x00select character_set_name from INFORMATION_SCHEMA.character_sets\x00\x00\x00B\x00\x00\x00\x0e\x00\x00\x00\x00\x00\x00\x00\x01\x00\x01D\x00\x00\x00\x06P\x00E\x00\x00\x00\t\x00\x00\x00\x00\x00S\x00\x00\x00\x04'

    # Tokenize input bytes stream
    tokens = tokenization(PBDES_Msg)

    # Parse messages to their attributes
    parsed_msgs = parse(tokens)

    print(parsed_msgs)

def PSQL_UT() :
    """
    Postgres command line utility Unit testing
    """
    # Example input
    PSQL_SIMPLE_QUERY_MSG = b'Q\x00\x00\x00\x19select * from test1;\x00'
    
    # Tokenize input bytes stream
    tokens = tokenization(PSQL_SIMPLE_QUERY_MSG)

    # Parse messages to their attributes
    parsed_msgs = parse(tokens)

    print(parsed_msgs)


if __name__ == "__main__" :
    # PBI_UT()

    PSQL_UT()
