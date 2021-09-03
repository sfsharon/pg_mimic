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

class Handler(SocketServer.BaseRequestHandler):
    def handle(self):
        print "handle()"
        self.read_SSLRequest()
        self.send_to_socket("N")

        self.read_StartupMessage()
        self.send_AuthenticationClearText()
        self.read_PasswordMessage()
        self.send_AuthenticationOK()
        self.send_ReadyForQuery()
        self.read_Query()
        self.send_queryresult()

    def RowDescription_Serialize(self, row_names):
        """! Serialize a row description section.

        @param row_names list of row names string

        @return packed bytes of rows description (T message)        

        RowDescription (B)

            Byte1('T')
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
            The format code being used for the field. Currently will be zero (text) or one (binary). In a RowDescription returned from the statement variant of Describe, the format code is not yet known and will always be zero.
        """
        MSG_ID = 'T'
        HEADERFORMAT = "!ih"
        ROWDESC_FORMAT = "!ihihih"

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

    def DataRow_Section_Serialize(self) :
        """
        Serialize a data row section.

        DataRow (B)
            Byte1('D')
            Identifies the message as a data row.

            Int32
            Length of message contents in bytes, including self.

            Int16
            The number of column values that follow (possibly zero).

            Next, the following pair of fields appear for each column:

            Int32
            The length of the column value, in bytes (this count does not include itself). Can be zero. As a special case, -1 indicates a NULL column value. No value bytes follow in the NULL case.

            Byten
            The value of the column, in the format indicated by the associated format code. n is the above length.
        """



    def send_queryresult(self):
        fieldnames = ['abc', 'def']
        HEADERFORMAT = "!cih"
        fields = ''.join(self.fieldname_msg(name) for name in fieldnames)
        rdheader = struct.pack(HEADERFORMAT, 'T', struct.calcsize(HEADERFORMAT) - 1 + len(fields), len(fieldnames))
        self.send_to_socket(rdheader + fields)

        rows = [[1, 2], [3, 4]]
        DRHEADER = "!cih"
        for row in rows:
            # dr_data = struct.pack("!ii", -1, -1)
            VALFORMAT = "!i"
            ROWFORMAT = "!iiii"
            dr_data = struct.pack(ROWFORMAT, struct.calcsize(VALFORMAT), row[0], struct.calcsize(VALFORMAT), row[1])
            dr_header = struct.pack(DRHEADER, 'D', struct.calcsize(DRHEADER) - 1 + len(dr_data), 2)
            self.send_to_socket(dr_header + dr_data)

        self.send_CommandComplete()
        self.send_ReadyForQuery()

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

    def send_AuthenticationOK(self):
        self.send_to_socket(struct.pack("!cii", 'R', 8, 0))

    def send_AuthenticationClearText(self):
        self.send_to_socket(struct.pack("!cii", 'R', 8, 3))


if __name__ == "__main__":
    server = SocketServer.TCPServer(("localhost", 9879), Handler)
    try:
        server.serve_forever()
    except:
        server.shutdown()

    # Testing
    # test_RowDescription_Serialize(['abc', 'def'])