#!/usr/bin/python3
"""
Session State machine.
Follows the reuiqred protocol messages, and keeps a state to respond corretly.
FSM : https://www.python-course.eu/finite_state_machine.php
Logging : https://docs.python.org/3/howto/logging.html
          https://realpython.com/python-logging/
Postgres data formats : https://www.postgresql.org/docs/12/protocol-message-formats.html          
"""

import logging
logging.basicConfig(level=logging.DEBUG)

"""##################3
#  TODO 16/10/2021 2030
  1. Complete the initialization session protocol - Receive the password message ('p'), and answerwith 
     a complete Status parameters to client.
"""
class StateMachine:    
    def __init__(self):
        self.handlers = {}
        self.new_state = None
        self.end_states = []

    def add_state(self, name, handler, end_state=0):
        name = name.upper()
        self.handlers[name] = handler
        if end_state:
            self.end_states.append(name)

    def set_start(self, name):
        self.new_state = name.upper()

    def run(self, cargo):
        try:
            handler = self.handlers[self.new_state.upper()]
        except:
            raise InitializationError("must call .set_start() before .run()")

        if not self.end_states:
            raise  InitializationError("at least one state must be an end_state")
    
        (self.new_state, cargo) = handler(cargo)
        if self.new_state.upper() in self.end_states:
            logging.info("reached ", self.new_state)
            return 
        else:
            handler = self.handlers[self.new_state.upper()] 

        return cargo

# ------------------------------------------------------------------------------------

# *****************************************************
# * Postgres Serialize / Deserialize functions
# *****************************************************
import struct

PARAMETER_STATUS_MSG_ID = bytes('S', "utf-8")
AUTHENTICATION_REQUEST_MSG_ID = bytes('R', "utf-8")
READY_FOR_QUERY_MSG_ID = bytes('Z', "utf-8")

READY_FOR_QUERY_SERVER_STATUS_IDLE = bytes('I', "utf-8")

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

    # Disregard user and password parameter/values

    msglen, protocol_major_ver, protocol_minor_ver = struct.unpack(HEADERFORMAT, data[0:8])

    logging.info("msglen : %d, protocol major : %d, protocol minor : %d", msglen, protocol_major_ver, protocol_minor_ver)

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
            param_name  + b'\x00' +                       \
            param_value + b'\x00'

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

def Z_Msg_ReadyForQuery_Serialize() :
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
    HEADERFORMAT = "!i"         # Length 

    Length = struct.calcsize(HEADERFORMAT) + len(READY_FOR_QUERY_SERVER_STATUS_IDLE)

    rVal = READY_FOR_QUERY_MSG_ID + struct.pack(HEADERFORMAT, Length) + READY_FOR_QUERY_SERVER_STATUS_IDLE

    return rVal

# *****************************************************
# * Postgres Protocol Implementation
# *****************************************************
STARTUP_STATE = "Startup_state"
PASSWORD_STATE = "Password_state"
PARAMETER_STATUS_STATE = "Parameter_status_state"
END_STATE = "End_state"

def startup_transition(txt) :
    logging.info("Enter startup_transition")

    # Deserialize Request
    Startup_Msg_Deserialize(txt)

    # Serialize Response
    send_msg = R_Msg_AuthRequest_Serialize()

    # Next state
    new_state = PASSWORD_STATE

    # TX Response
    return (new_state, send_msg)

def password_state_transition(txt) :
    logging.info("Enter password_state_transition")

    # TODO : perform password authentication

    # Deserialize Request
    # TBD

    # Serialize Response
    send_msg = ""

    # Next state
    new_state = PARAMETER_STATUS_STATE

    # TX Response
    return (new_state, send_msg)

def patameter_status_state_transition(txt) :
    logging.info("Enter patameter_status_state_transition")

    # Deserialize Request
    #N/A

    # Serialize Response    
    send_msg = R_Msg_AuthOk_Serialize()

    send_msg  = S_Msg_ParameterStatus_Serialize (str.encode('client_encoding'), str.encode('UTF8'))
    send_msg += S_Msg_ParameterStatus_Serialize (str.encode('DateStyle'), str.encode('ISO, MDY'))
    send_msg += S_Msg_ParameterStatus_Serialize (str.encode('integer_datetimes'), str.encode('on'))
    send_msg += S_Msg_ParameterStatus_Serialize (str.encode('IntervalStyle'), str.encode('postgres'))
    send_msg += S_Msg_ParameterStatus_Serialize (str.encode('is_superuser'), str.encode('on'))
    send_msg += S_Msg_ParameterStatus_Serialize (str.encode('server_encoding'), str.encode('UTF8'))
    send_msg += S_Msg_ParameterStatus_Serialize (str.encode('server_version'), str.encode('12.7'))
    send_msg += S_Msg_ParameterStatus_Serialize (str.encode('session_authorization'), str.encode('postgres'))
    send_msg += S_Msg_ParameterStatus_Serialize (str.encode('standard_conforming_strings'), str.encode('on'))

    send_msg += Z_Msg_ReadyForQuery_Serialize()

    # Next state
    new_state = END_STATE

    # TX Response
    return (new_state, send_msg)


# *****************************************************
# * PG server logic
# *****************************************************
import socketserver


class MyPGHandler(socketserver.BaseRequestHandler):
    """
    The request handler class for Postgres mimic server.
    """
    INPUT_BUFF_SIZE = 1024 * 1024

    def handle(self):
        while True :
            # RX Request
            self.data = self.request.recv(self.INPUT_BUFF_SIZE)

            # Debug info
            logging.debug("{} wrote:".format(self.client_address[0]))
            logging.debug(self.data)

            # Run State Machine
            send_data = ""
            # Run state machine as long as the state transitions have nothing to transmit
            while send_data == "" :
                send_data = self.server.pg_sm.run(self.data)

            # TX Response
            self.request.sendall(send_data)

def CreatePGStateMachine() :
    pg_mimic = StateMachine()
    pg_mimic.add_state(STARTUP_STATE, startup_transition)
    pg_mimic.add_state(PASSWORD_STATE, password_state_transition)
    pg_mimic.add_state(PARAMETER_STATUS_STATE, patameter_status_state_transition)
    pg_mimic.add_state(END_STATE, None, end_state=1)
    pg_mimic.set_start(STARTUP_STATE)

    return pg_mimic


def RunPGServer(host, port) :

    # Create the server, binding to localhost on port PG_PORT
    with socketserver.TCPServer((host, port), MyPGHandler) as server:
        server.pg_sm = CreatePGStateMachine()

        # Activate the server; this will keep running until you
        # interrupt the program with Ctrl-C
        logging.info("Starting PG mimic server")
        server.serve_forever()        

# *****************************************************
# * Main Functionality
# *****************************************************
if __name__ == "__main__" :
    PG_PORT = 5432
    HOST, PORT = "localhost", PG_PORT
    RunPGServer(HOST, PORT)
