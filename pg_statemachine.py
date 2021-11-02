#!/usr/bin/python3
"""
Implements Postgres wire protocol state machine
FSM : https://www.python-course.eu/finite_state_machine.php
Postgres data formats : https://www.postgresql.org/docs/12/protocol-message-formats.html          
"""

import logging
logging.basicConfig(level=logging.DEBUG)

# *****************************************************
# * General State machine implementation
# *****************************************************
class StateMachine:    
    def __init__(self):
        self.handlers = {}
        self.new_state = None
        self.backend_db_con = None

    def add_state(self, name, handler, end_state=0):
        name = name.upper()
        self.handlers[name] = handler

    def set_start(self, name):
        self.new_state = name

    def run(self, parsed_msgs):
        try:
            handler = self.handlers[self.new_state]
        except:
            raise InitializationError("must call .set_start() before .run()")

        #Process the first message in parsed messages list
        curr_msg = {}
        if len(parsed_msgs) > 0 :
            curr_msg = parsed_msgs[0]
        else :
            logging.info(f"Executing state {self.new_state} without input")

        # Run state logic
        (self.new_state, send_msg) = handler(curr_msg, self.backend_db_con)

        # Remove digested message from parsed messages list
        parsed_msgs = parsed_msgs[1:]

        # Update next state logic handler
        handler = self.handlers[self.new_state] 

        return send_msg, parsed_msgs

# ------------------------------------------------------------------------------------

from pg_serdes import *
from sqream_backend import *

# *****************************************************
# * Postgres Protocol Implementation
# *****************************************************
STARTUP_STATE = "STARTUP_STATE"
PASSWORD_STATE = "PASSWORD_STATE"
PARAMETER_STATUS_STATE = "PARAMETER_STATUS_STATE"
QUERY_STATE = "QUERY_STATE"
END_STATE = "END_STATE"

def startup_transition(txt, backend_db_con) :
    logging.info("Enter startup_transition")


    # Serialize Response
    send_msg = R_Msg_AuthRequest_Serialize()

    # Next state
    new_state = PASSWORD_STATE

    # TX Response
    return (new_state, send_msg)

def password_state_transition(msg, backend_db_con) :
    # Verify this is password message
    if not is_passwd_msg(msg) :
        logging.info("password_state_transition: Did not get password message. Returning to Startup")
        # Serialize Response
        send_msg = ""
        # Next state
        new_state = STARTUP_STATE
    else :
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

def patameter_status_state_transition(msg, backend_db_con) :
    """! Builds parameter status message during intialization phase.
         Does not take into account the password (the msg parameter)
    @param msg password 

    @return parameter status message
    
    """
    logging.info("Enter patameter_status_state_transition")

    # Deserialize Request
    #N/A

    # Serialize Response    
    send_msg = R_Msg_AuthOk_Serialize()

    send_msg += S_Msg_ParameterStatus_Serialize (str.encode('client_encoding'), str.encode('UTF8'))
    send_msg += S_Msg_ParameterStatus_Serialize (str.encode('DateStyle'), str.encode('ISO, MDY'))
    send_msg += S_Msg_ParameterStatus_Serialize (str.encode('integer_datetimes'), str.encode('on'))
    send_msg += S_Msg_ParameterStatus_Serialize (str.encode('IntervalStyle'), str.encode('postgres'))
    send_msg += S_Msg_ParameterStatus_Serialize (str.encode('is_superuser'), str.encode('on'))
    send_msg += S_Msg_ParameterStatus_Serialize (str.encode('server_encoding'), str.encode('UTF8'))
    send_msg += S_Msg_ParameterStatus_Serialize (str.encode('server_version'), str.encode('12.7'))
    send_msg += S_Msg_ParameterStatus_Serialize (str.encode('session_authorization'), str.encode('postgres'))
    send_msg += S_Msg_ParameterStatus_Serialize (str.encode('standard_conforming_strings'), str.encode('on'))

    send_msg += Z_Msg_ReadyForQuery_Serialize(READY_FOR_QUERY_SERVER_STATUS_IDLE)

    # Next state
    new_state = QUERY_STATE

    # TX Response
    return (new_state, send_msg)

def query_state_transition(msg, backend_db_con) :
    """! Performs query.
    @param msg password string

    @return parameter result of query
    
    """
    logging.info("Enter query_state_transition")

    query = msg[QUERY_MSG__SIMPLE_QUERY]

    # Query backend database
    result = execute_query(backend_db_con, query)

    # Serialize Response    
    send_msg = T_Msg_RowDescription_Serialize(['xint']) 

    for row_val in result :
        send_msg += D_Msg_DataRow_Serialize(row_val) 


    send_msg += C_Msg_CommandComplete_Serialize('SELECT 3') 
    send_msg += Z_Msg_ReadyForQuery_Serialize(READY_FOR_QUERY_SERVER_STATUS_IDLE)

    # Next state
    new_state = QUERY_STATE

    # TX Response
    return (new_state, send_msg)

# ---------------------------------------------------------------------------------------------
HOST = "192.168.4.64"
PORT = 5000
DATABASE = "master"
CLUSTERED = False
USERNAME = "sqream"
PASSWORD = "sqream"

# Put it all together
def CreatePGStateMachine() :
    pg_mimic = StateMachine()
    pg_mimic.add_state(STARTUP_STATE, startup_transition)
    pg_mimic.add_state(PASSWORD_STATE, password_state_transition)
    pg_mimic.add_state(PARAMETER_STATUS_STATE, patameter_status_state_transition)
    pg_mimic.add_state(QUERY_STATE, query_state_transition)
    pg_mimic.add_state(END_STATE, None, end_state=1)
    pg_mimic.set_start(STARTUP_STATE)

    pg_mimic.backend_db_con = get_db(   host = HOST, port = PORT, 
                                        database = DATABASE, 
                                        username = USERNAME, password = PASSWORD)

    return pg_mimic

def is_initial_state(sm) :
    """
    Input : Receives a  StateMachine()
    Output : True if state is STARTUP_STATE, False otherwise.
    """
    return True if sm.new_state == STARTUP_STATE else False

def force_initial_state(sm) :
    """
    Forces the state machine to return to initial state, in case session ended
    Input : Receives a  StateMachine()
    Output : N/A
    """
    sm.new_state = STARTUP_STATE