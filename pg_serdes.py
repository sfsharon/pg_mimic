#!/usr/bin/python3
"""
Postgres Serialize-Deserialize messages module.
Postgres data formats : https://www.postgresql.org/docs/12/protocol-message-formats.html          
"""

import logging
logging.basicConfig(level=logging.DEBUG)

import struct

# Message ID constants
PARAMETER_STATUS_MSG_ID = bytes('S', "utf-8")
AUTHENTICATION_REQUEST_MSG_ID = bytes('R', "utf-8")
READY_FOR_QUERY_MSG_ID = bytes('Z', "utf-8")
QUERY_MSG_ID = bytes('Q', "utf-8")
CMD_COMPLETE_MSG_ID = bytes('C', "utf-8")
DATA_COLS_MSG_ID = bytes('D', "utf-8")
ROW_DESC_MSG_ID = bytes('T', "utf-8")

# Server state constants
READY_FOR_QUERY_SERVER_STATUS_IDLE = bytes('I', "utf-8")

# Row description constants
INT_TYPE_OID = 23
INT_TYPE_COLUMN_LENGTH = 4
TYPE_FORMAT_TEXT = 0
TYPE_FORMAT_BINARY = 1

# Misc constants
NULL_TERMINATOR = b'\x00'

# ***********************************************
# * Utility functions
# ***********************************************
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

# ***********************************************
# * Serialize / Deserialize functions
# ***********************************************

def Startup_Msg_Deserialize(data) :
    """! Deserialize startup message
    @param N/A

    @return N/A

    StartupMessage (Frontend)
        Int32
        Length of message contents in bytes, including self.

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
    HEADERFORMAT = "!ihh"     # Length / Protocol major ver / Protocol minor ver  

    # Disregard user parameter/value

    msglen, protocol_major_ver, protocol_minor_ver = struct.unpack(HEADERFORMAT, data[0:8])

    logging.info("msglen : %d, protocol major : %d, protocol minor : %d", msglen, protocol_major_ver, protocol_minor_ver)

def Q_Msg_Query_Deserialize(data) :
    """! Deserialize simple query message
    @param data bytes array of the simple query

    @return query string

    Query (Frontend)
        Byte1('Q')
        Identifies the message as a simple query.

        Int32
        Length of message contents in bytes, including self.

        String
        The query string itself.        
    """
    HEADERFORMAT = "!ci"     # MsgID / Length
    header_length = struct.calcsize(HEADERFORMAT)
    msg_id, msg_len = struct.unpack(HEADERFORMAT, data[0:header_length])
    assert msg_id == QUERY_MSG_ID
    query = data[header_length:]

    logging.info("*** Q_Msg_Query_Deserialize: Query received \"{}\"".format(query))

    return query

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

def D_Msg_DataRow_Serialize(col_values) :
    """! Serialize a data col section.
    @param row_values list of column values

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

    rVal = bytes('', "utf-8")

    Field_count = len(col_values)

    for count, col_value in enumerate (col_values) :
        if isinstance(col_value, int):
            col_value_string = utility_int_to_text(col_value)
        else:
            col_value_string = bytes(col_value, "utf-8")
        rVal += struct.pack(COLDESC_FORMAT, len(col_value_string)) + col_value_string

    Length = struct.calcsize(HEADERFORMAT) + len(rVal)

    rVal = DATA_COLS_MSG_ID + struct.pack(HEADERFORMAT, Length, Field_count) + rVal

    return rVal


def T_Msg_RowDescription_Serialize(row_names):
        """! Serialize a row description section.

        @param row_names list of row names string

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

        rVal = bytes('', "utf-8")

        Field_count = len(row_names)

        for count, row_name in enumerate (row_names) :            
            null_term_row_name = bytes(row_name, "utf-8") + NULL_TERMINATOR
            
            Table_OID = 49152               # Hard coded value
            Column_index = count + 1
            Type_OID = INT_TYPE_OID                 # Hard coded value. More information on OID Types : https://www.postgresql.org/docs/9.4/datatype-oid.html
            Column_length = INT_TYPE_COLUMN_LENGTH  # Hard coded value. Fits Int type size.
            Type_modifier = -1                      # Hard coded value. 
            Format = TYPE_FORMAT_TEXT               # Hard coded value. Fits text format.
            rVal += null_term_row_name + struct.pack(ROWDESC_FORMAT, 
                                                    Table_OID, 
                                                    Column_index, 
                                                    Type_OID, 
                                                    Column_length, 
                                                    Type_modifier, 
                                                    Format)

        Length = struct.calcsize(HEADERFORMAT) + len(rVal)

        rVal = ROW_DESC_MSG_ID + struct.pack(HEADERFORMAT, Length, Field_count) + rVal

        return rVal

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

    rVal = CMD_COMPLETE_MSG_ID + struct.pack(HEADERFORMAT, Length) + tag_name_null_term

    return rVal

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

    rVal = READY_FOR_QUERY_MSG_ID + struct.pack(HEADERFORMAT, Length) + READY_FOR_QUERY_SERVER_STATUS_IDLE

    return rVal
