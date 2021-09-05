# Taken from :            https://stackoverflow.com/questions/335008/creating-a-custom-odbc-driver
# Postgres data formats : https://www.postgresql.org/docs/12/protocol-message-formats.html

import SocketServer
import struct

def char_to_hex(char):
    retval = hex(ord(char))
    if len(retval) == 4:
        return retval[-2:]
    else:
        assert len(retval) == 3
        return "0" + retval[-1]

def str_to_hex(inputstr):
    return " ".join(char_to_hex(char) for char in inputstr)

def utility_int_to_text(val) :
    """! Translate a string to an ordinal string, little endian
    @param val integer to translate

    @return string comprises of list of ordinals ascii value, representing the input val
            For example : int value 192, return value 0x31/0x39/0x32
    """
    rVal = ''
    while val != 0 :
        digit = val % 10
        val = val / 10
        char_digit = struct.pack("!c",  str(digit))
        rVal += char_digit
    rVal = rVal[::-1]           # Reverse the string
    return rVal

class Handler(SocketServer.BaseRequestHandler):

    curr_query = ''

    def T_Msg_RowDescription_Serialize(self, row_names):
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
        MSG_ID = 'T'                    # Type
        HEADERFORMAT = "!ih"            # Length / Field count
        ROWDESC_FORMAT = "!ihihih"      # Table OID / Column index / Type OID / Column length / Type modifier / Format

        rVal = ''

        Field_count = len(row_names)

        for count, row_name in enumerate (row_names) :
            null_ter_row_name = row_name + b'\x00'
            Table_OID = 49152               # Hard coded value
            Column_index = count + 1
            Type_OID = 23                   # Hard coded value. More information on OID Types : https://www.postgresql.org/docs/9.4/datatype-oid.html
            Column_length = 4               # Hard coded value. Fits Int type size.
            Type_modifier = -1              # Hard coded value. 
            Format = 0                      # Hard coded value. Fits text format.
            rVal += null_ter_row_name + struct.pack(ROWDESC_FORMAT, 
                                                    Table_OID, 
                                                    Column_index, 
                                                    Type_OID, 
                                                    Column_length, 
                                                    Type_modifier, 
                                                    Format)

        Length = struct.calcsize(HEADERFORMAT) + len(rVal)

        rVal = MSG_ID + struct.pack(HEADERFORMAT, Length, Field_count) + rVal

        return rVal

    def D_Msg_DataRow_Serialize(self, col_values) :
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

        MSG_ID = 'D'                # Type
        HEADERFORMAT = "!ih"        # Length / Field count
        COLDESC_FORMAT = "!i"       # Column length

        rVal = ''

        Field_count = len(col_values)

        for count, col_value in enumerate (col_values) :
            col_value_string = utility_int_to_text(col_value)
            rVal += struct.pack(COLDESC_FORMAT, len(col_value_string)) + col_value_string

        Length = struct.calcsize(HEADERFORMAT) + len(rVal)

        rVal = MSG_ID + struct.pack(HEADERFORMAT, Length, Field_count) + rVal

        return rVal

    def C_Msg_CommandComplete_Serialize(self, tag_name) :
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

            For an INSERT command, the tag is INSERT oid rows, where rows is the number of rows inserted. oid used to be the object ID of the inserted row if rows was 1 and the target table had OIDs, but OIDs system columns are not supported anymore; therefore oid is always 0.

            For a DELETE command, the tag is DELETE rows where rows is the number of rows deleted.

            For an UPDATE command, the tag is UPDATE rows where rows is the number of rows updated.

            For a SELECT or CREATE TABLE AS command, the tag is SELECT rows where rows is the number of rows retrieved.

            For a MOVE command, the tag is MOVE rows where rows is the number of rows the cursor's position has been changed by.

            For a FETCH command, the tag is FETCH rows where rows is the number of rows that have been retrieved from the cursor.

            For a COPY command, the tag is COPY rows where rows is the number of rows copied. (Note: the row count appears only in PostgreSQL 8.2 and later.)

        """

        MSG_ID = 'C'                # Type
        HEADERFORMAT = "!i"         # Length 

        Length = struct.calcsize(HEADERFORMAT) + len(tag_name)        

        rVal = MSG_ID + struct.pack(HEADERFORMAT, Length) + tag_name

        return rVal

    def Z_Msg_ReadyForQuery_Serialize(self) :
        """! Serialize a ready for query section.
        @param N/A

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

        MSG_ID = 'Z'                # Type
        HEADERFORMAT = "!i"         # Length 
        Status = 'T'

        Length = struct.calcsize(HEADERFORMAT) + len(Status)

        rVal = MSG_ID + struct.pack(HEADERFORMAT, Length) + Status

        return rVal

    def S_Msg_ParameterStatus_Serialize(self, param_name, param_value) :
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

        MSG_ID = 'S'                # Type
        HEADERFORMAT = "!i"         # Length 

        Length = struct.calcsize(HEADERFORMAT) +    \
                 len(param_name) + 1 +              \
                 len(param_value) + 1 

        rVal = MSG_ID + struct.pack(HEADERFORMAT, Length) + \
               param_name  + b'\x00' +                       \
               param_value + b'\x00'

        return rVal


    def Q_Msg_Query_Deserialize(self) :
        """! Deserialize Query message
        @param N/A

        @return Query string

        Query (Frontend)
            Byte1('Q')
            Identifies the message as a simple query.

            Int32
            Length of message contents in bytes, including self.

            String
            The query string itself.
        """
        data = self.read_socket()

        HEADERFORMAT = "!ci"     # MsgID / Length
        header_length = struct.calcsize(HEADERFORMAT)
        msg_ident, msg_len = struct.unpack(HEADERFORMAT, data[0:header_length])
        assert msg_ident == "Q"
        self.curr_query = data[header_length:]

        print "*** Q_Msg_Query_Deserialize: Query received \"{}\"".format(self.curr_query)


    def Startup_Msg_Deserialize(self) :
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
        data = self.read_socket()

        HEADERFORMAT = "!ihh"     # Length / Protocol major ver / Protocol minor ver  

        # Disregard user and password parameter/values

        msglen, protocol_major_ver, protocol_minor_ver = struct.unpack(HEADERFORMAT, data[0:8])

        print "*** Startup_Msg_Deserialize: Major Ver {} Minor Ver {}".format(protocol_major_ver, protocol_minor_ver)

            # Original implementation
            # ------------------------------------------------------------------
            # def read_StartupMessage(self):
            #     data = self.read_socket()
            #     msglen, protoversion = struct.unpack("!ii", data[0:8])
            #     print "msglen: {}, protoversion: {}".format(msglen, protoversion)
            #     assert msglen == len(data)
            #     parameters_string = data[8:]
            #     print parameters_string.split('\x00')


    def handle(self):
        print "*** handle()"
        self.Startup_Msg_Deserialize()
        self.send_AuthenticationClearText()
        self.read_PasswordMessage()
        self.send_AuthenticationOK_and_param_status()
        self.send_ReadyForQuery()
        while True :
            self.Q_Msg_Query_Deserialize()
            self.send_queryresult()

    def prepare_parameter_status(self) :
        msg  = self.S_Msg_ParameterStatus_Serialize ('client_encoding', 'UTF8')
        msg += self.S_Msg_ParameterStatus_Serialize ('DateStyle', 'ISO, MDY')
        msg += self.S_Msg_ParameterStatus_Serialize ('integer_datetimes', 'on')
        msg += self.S_Msg_ParameterStatus_Serialize ('IntervalStyle', 'postgres')                
        msg += self.S_Msg_ParameterStatus_Serialize ('is_superuser', 'on')
        msg += self.S_Msg_ParameterStatus_Serialize ('server_encoding', 'UTF8')                
        msg += self.S_Msg_ParameterStatus_Serialize ('server_version', '12.7')
        msg += self.S_Msg_ParameterStatus_Serialize ('session_authorization', 'postgres')        
        msg += self.S_Msg_ParameterStatus_Serialize ('standard_conforming_strings', 'on')                

        return msg      

    def send_queryresult(self):
        query_result = ''

        if 'BEGIN' in self.curr_query :
            cmd_complete = self.C_Msg_CommandComplete_Serialize('BEGIN' + b'\x00')              
            ready_for_query = self.Z_Msg_ReadyForQuery_Serialize()

            query_result = cmd_complete + ready_for_query
        else :  # Assumes 'SELECT' query
            row_desc   =  self.T_Msg_RowDescription_Serialize (['abc', 'def'])  
            data_row_1 = self.D_Msg_DataRow_Serialize([1, 2])                   
            data_row_2 = self.D_Msg_DataRow_Serialize([193, 456])
            data_row_3 = self.D_Msg_DataRow_Serialize([842, 843])

            num_of_rows_string = utility_int_to_text(3)
            tag_name = "SELECT " + num_of_rows_string + b'\x00' 
            cmd_complete = self.C_Msg_CommandComplete_Serialize(tag_name)        
            ready_for_query = self.Z_Msg_ReadyForQuery_Serialize()

            query_result = row_desc     + \
                        data_row_1   + \
                        data_row_2   + \
                        data_row_3   + \
                        cmd_complete + \
                        ready_for_query

        self.send_to_socket(query_result)

    def send_CommandComplete(self):
        HFMT = "!ci"
        msg = "SELECT 2\x00"
        self.send_to_socket(struct.pack(HFMT, "C", struct.calcsize(HFMT) - 1 + len(msg)) + msg)

    def fieldname_msg(self, name):
        tableid = 0
        columnid = 0
        datatypeid = 23
        datatypesize = 4
        typemodifier = -1
        format_code = 0 # 0=text 1=binary
        return name + "\x00" + struct.pack("!ihihih", tableid, columnid, datatypeid, datatypesize, typemodifier, format_code)

    def read_socket(self):
        print "Trying recv..."
        data = self.request.recv(1024)
        print "Received {} bytes: {}".format(len(data), repr(data))
        print "Hex: {}".format(str_to_hex(data))
        return data

    def send_to_socket(self, data):
        print "Sending {} bytes: {}".format(len(data), repr(data))
        print "Hex: {}".format(str_to_hex(data))
        return self.request.sendall(data)

    def read_Query(self):
        data = self.read_socket()
        msgident, msglen = struct.unpack("!ci", data[0:5])
        assert msgident == "Q"
        print data[5:]


    def send_ReadyForQuery(self):
        self.send_to_socket(struct.pack("!cic", 'Z', 5, 'I'))

    def read_PasswordMessage(self):
        data = self.read_socket()
        b, msglen = struct.unpack("!ci", data[0:5])
        assert b == "p"
        print "Password: {}".format(data[5:])


    def read_SSLRequest(self):
        data = self.read_socket()
        msglen, sslcode = struct.unpack("!ii", data)
        assert msglen == 8
        assert sslcode == 80877103

    def read_StartupMessage(self):
        data = self.read_socket()
        msglen, protoversion = struct.unpack("!ii", data[0:8])
        print "msglen: {}, protoversion: {}".format(msglen, protoversion)
        assert msglen == len(data)
        parameters_string = data[8:]
        print parameters_string.split('\x00')

    def send_AuthenticationClearText(self):
        self.send_to_socket(struct.pack("!cii", 'R', 8, 3))

    def send_AuthenticationOK_and_param_status(self):
        param_status = self.prepare_parameter_status()
        self.send_to_socket(struct.pack("!cii", 'R', 8, 0) + param_status)




if __name__ == "__main__":
    server = SocketServer.TCPServer(("localhost", 9879), Handler)
    try:
        print "*** Waiting for connection"
        server.serve_forever()
    except:
        print "*** Shutting down"
        server.shutdown()
