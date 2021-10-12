#!/usr/bin/python3
"""
Session State machine.
Follows the reuiqred protocol messages, and keeps a state to respond corretly.
Based on : https://www.python-course.eu/finite_state_machine.php
"""


"""##################3
#  TODO 12/10/2021 0741
  1. Add the communication object (server)
  2. Complete the initialization session protocol.   
"""
class StateMachine:    
    def __init__(self):
        self.handlers = {}
        self.startState = None
        self.endStates = []

    def add_state(self, name, handler, end_state=0):
        name = name.upper()
        self.handlers[name] = handler
        if end_state:
            self.endStates.append(name)

    def set_start(self, name):
        self.startState = name.upper()

    def run(self, cargo):
        try:
            handler = self.handlers[self.startState]
        except:
            raise InitializationError("must call .set_start() before .run()")
        if not self.endStates:
            raise  InitializationError("at least one state must be an end_state")
    
        while True:
            (newState, cargo) = handler(cargo)
            if newState.upper() in self.endStates:
                print("reached ", newState)
                break 
            else:
                handler = self.handlers[newState.upper()] 

# ------------------------------------------------------------------------------------

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
    # data = self.read_socket()

    HEADERFORMAT = "!ihh"     # Length / Protocol major ver / Protocol minor ver  

    # Disregard user and password parameter/values

    msglen, protocol_major_ver, protocol_minor_ver = struct.unpack(HEADERFORMAT, data[0:8])

    print ("msglen : {}, protocol major : {}, protocol minor : {}", format(msglen, protocol_major_ver, protocol_minor_ver))

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

    MSG_ID = 'S'                # Type
    HEADERFORMAT = "!i"         # Length 

    Length = struct.calcsize(HEADERFORMAT) +    \
                len(param_name) + 1 +              \
                len(param_value) + 1 

    rVal = MSG_ID + struct.pack(HEADERFORMAT, Length) + \
            param_name  + b'\x00' +                       \
            param_value + b'\x00'

    return rVal

def AuthRequest_Msg_Serialize():
    # param_status  = S_Msg_ParameterStatus_Serialize ('client_encoding', 'UTF8')
    # param_status += S_Msg_ParameterStatus_Serialize ('DateStyle', 'ISO, MDY')
    # param_status += S_Msg_ParameterStatus_Serialize ('integer_datetimes', 'on')
    # param_status += S_Msg_ParameterStatus_Serialize ('IntervalStyle', 'postgres')                
    # param_status += S_Msg_ParameterStatus_Serialize ('is_superuser', 'on')
    # param_status += S_Msg_ParameterStatus_Serialize ('server_encoding', 'UTF8')                
    # param_status += S_Msg_ParameterStatus_Serialize ('server_version', '12.7')
    # param_status += S_Msg_ParameterStatus_Serialize ('session_authorization', 'postgres')    
    # param_status += S_Msg_ParameterStatus_Serialize ('standard_conforming_strings', 'on')                

    auth_req_OK = struct.pack("!cii", 'R', 8, 0) # Authentication Request OK
    # read_for_query = self.Z_Msg_ReadyForQuery_Serialize()
    # rVal = auth_req_OK + param_status + read_for_query
    return auth_req_OK

def startup_transition() :
    # RX Request
    txt = comm.rx()

    # Deserialize Request
    Startup_Msg_Deserialize(txt)

    # Serialize Response
    send_msg = AuthRequest_Msg_Serialize()

    # TX Response
    comm.tx(send_msg)

    # Next state
    newState = "password_state"

def password_state_transition() :
    # RX Request
    txt = comm.rx()
    print(txt)

    # # Deserialize Request
    # Startup_Msg_Deserialize(txt)

    # # Serialize Response
    # send_msg = AuthRequest_Msg_Serialize()

    # # TX Response
    # comm.tx(send_msg)

    # # Next state
    # newState = "password_state"


if __name__ == "__main__" :
    pg_mimic = StateMachine()
    pg_mimic.add_state("Startup", startup_transition)
    pg_mimic.add_state("Password_state", password_state_transition)
    pg_mimic.run()