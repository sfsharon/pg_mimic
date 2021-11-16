#!/usr/bin/python3
"""
Implements Postgres wire protocol state machine
FSM : https://www.python-course.eu/finite_state_machine.php
Postgres network messages formats : https://www.postgresql.org/docs/12/protocol-message-formats.html          
"""

import logging
logging.basicConfig(level=logging.DEBUG)

# *****************************************************
# * State machine constants
# *****************************************************
STATE_MACHINE__NEW_STATE = "new_state"
STATE_MACHINE__OUTPUT_MSG = "send_msg"
STATE_MACHINE__IS_TX_MSG = "is_tx_msg"
STATE_MACHINE__PARSED_MSGS = "parsed_msgs"

# ********************************************************
# * PG communication protocol State machine implementation
# ********************************************************
class PG_StateMachine:    
    def __init__(self):
        self.handlers = {}
        self.new_state = None
        self.backend_db_con = None

    def add_state(self, name, handler, end_state=0):
        name = name.upper()
        self.handlers[name] = handler

    def set_start(self, name):
        self.new_state = name

    def run(self, parsed_msgs, output_msg):
        """
        parsed_msgs : List of dictionaries, holding the input mesages
        output_msg : A string being built by the states in the state machine.
        """

        res = {}

        handler = self.handlers[self.new_state]

        # Run state logic
        res = handler(  parsed_msgs, 
                        output_msg, 
                        self.backend_db_con)        

        # Update next state logic handler
        self.new_state = res[STATE_MACHINE__NEW_STATE] 

        return res

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
SIMPLE_QUERY_STATE = "SIMPLE_QUERY_STATE"
PARSE_QUERY_STATE = "PARSE_QUERY_STATE"
END_STATE = "END_STATE"

def startup_transition(parsed_msgs, output_msg, backend_db_con) :
    logging.info("Entering startup_transition")

    res = {}

    assert len(parsed_msgs) > 0, "Receied an empty input parsed messages"
    input_msg = parsed_msgs[0]
    # "munch" the input parsed_msgs
    res[STATE_MACHINE__PARSED_MSGS] = parsed_msgs[1:]

    # Serialize Response
    res[STATE_MACHINE__OUTPUT_MSG] = R_Msg_AuthRequest_Serialize()

    # Send response immediately
    res[STATE_MACHINE__IS_TX_MSG] = True

    # Next state
    res[STATE_MACHINE__NEW_STATE] = PASSWORD_STATE # new_state = PASSWORD_STATE

    return res

def password_state_transition(parsed_msgs, output_msg, backend_db_con) :
    res = {}

    assert len(parsed_msgs) > 0, "Receied an empty input parsed messages"
    input_msg = parsed_msgs[0]
    # "munch" the input parsed_msgs
    res[STATE_MACHINE__PARSED_MSGS] = parsed_msgs[1:]

    # Serialize Response
    res[STATE_MACHINE__OUTPUT_MSG] = output_msg # send_msg = ""

    # Verify this is password message
    if not is_password_msg(input_msg) :
        logging.info("password_state_transition: Did not get password message. Returning to Startup")
        
        # Next state
        res[STATE_MACHINE__NEW_STATE] = STARTUP_STATE

    else :
        logging.info("Entering password_state_transition")

        # Next state
        res[STATE_MACHINE__NEW_STATE] = PARAMETER_STATUS_STATE # new_state = PARAMETER_STATUS_STATE

    res[STATE_MACHINE__IS_TX_MSG] = False

    return res

def patameter_status_state_transition(parsed_msgs, output_msg, backend_db_con) :
    """! Builds parameter status message during intialization phase.
         Does not take into account the password (the msg parameter)
    @param msg password 

    @return parameter status message
    
    """
    logging.info("Entering patameter_status_state_transition")

    res = {}

    # Serialize Response    
    output_msg = R_Msg_AuthOk_Serialize()

    output_msg += S_Msg_ParameterStatus_Serialize (str.encode('client_encoding'), str.encode('UTF8'))
    output_msg += S_Msg_ParameterStatus_Serialize (str.encode('DateStyle'), str.encode('ISO, MDY'))
    output_msg += S_Msg_ParameterStatus_Serialize (str.encode('integer_datetimes'), str.encode('on'))
    output_msg += S_Msg_ParameterStatus_Serialize (str.encode('IntervalStyle'), str.encode('postgres'))
    output_msg += S_Msg_ParameterStatus_Serialize (str.encode('is_superuser'), str.encode('on'))
    output_msg += S_Msg_ParameterStatus_Serialize (str.encode('server_encoding'), str.encode('UTF8'))
    output_msg += S_Msg_ParameterStatus_Serialize (str.encode('server_version'), str.encode('12.7'))
    output_msg += S_Msg_ParameterStatus_Serialize (str.encode('session_authorization'), str.encode('postgres'))
    output_msg += S_Msg_ParameterStatus_Serialize (str.encode('standard_conforming_strings'), str.encode('on'))

    output_msg += Z_Msg_ReadyForQuery_Serialize(READY_FOR_QUERY_SERVER_STATUS_IDLE)

    res[STATE_MACHINE__OUTPUT_MSG] = output_msg
    res[STATE_MACHINE__IS_TX_MSG] = True

    # Next state
    res[STATE_MACHINE__NEW_STATE] = QUERY_STATE # new_state = QUERY_STATE

    return res

def query_state_transition(parsed_msgs, output_msg, backend_db_con) :
    """! In-between state, to decide if this is a simple or parse message.
    @param msg password string

    @return N/A
    
    """
    logging.info("Entering query_state_transition")

    res = {}
    assert len(parsed_msgs) > 0, "Receied an empty input parsed messages"
    input_msg = parsed_msgs[0]
    # Do not "munch" on the parsed_msgs
    res[STATE_MACHINE__PARSED_MSGS] = parsed_msgs

    is_tx_msg = False

    # Update logic
    if input_msg[MSG_ID] == QUERY_MSG_ID :        
        res[STATE_MACHINE__NEW_STATE] = SIMPLE_QUERY_STATE 
    elif input_msg[MSG_ID] == PARSE_MSG_ID :
        res[STATE_MACHINE__NEW_STATE] = PARSE_QUERY_STATE
    elif input_msg[MSG_ID] == SYNC_MSG_ID :
        res[STATE_MACHINE__NEW_STATE] = QUERY_STATE
        is_tx_msg = True
    else :
        raise ValueError('Received unknown message ID : ', input_msg[MSG_ID])

    res[STATE_MACHINE__OUTPUT_MSG] = output_msg
    res[STATE_MACHINE__IS_TX_MSG] = is_tx_msg

    return res 

def simple_query_state_transition(parsed_msgs, output_msg, backend_db_con) :
    """! Performs simple query. Use case : Activated from the psql client
    @param msg password string

    @return parameter result of query
    
    """
    logging.info("Entering simple_query_state_transition")

    res = {}

    assert len(parsed_msgs) > 0, "Receied an empty input parsed messages"
    input_msg = parsed_msgs[0]
    res[STATE_MACHINE__PARSED_MSGS] = parsed_msgs[1:]
    assert (input_msg[MSG_ID] == QUERY_MSG_ID), f"Received a wrong message ID: {msg[MSG_ID]} insteadt of {QUERY_MSG_ID}"
    
    query = input_msg[QUERY_MSG__SIMPLE_QUERY]

    # Query backend database
    query_output = execute_query(backend_db_con, query)

    cols_desc = query_output[BACKEND_QUERY__DESCRIPTION]
    result    = query_output[BACKEND_QUERY__RESULT]

    # Serialize Response
    msg = T_Msg_RowDescription_Serialize(cols_desc) 

    for cols_values in result :
        msg += D_Msg_DataRow_Serialize(cols_desc, cols_values) 

    num_of_lines = len(result)

    msg += C_Msg_CommandComplete_Serialize('SELECT ' + str(num_of_lines)) 

    msg += Z_Msg_ReadyForQuery_Serialize(READY_FOR_QUERY_SERVER_STATUS_IDLE)

    res[STATE_MACHINE__OUTPUT_MSG] = output_msg + msg
    res[STATE_MACHINE__IS_TX_MSG] = True

    # Next state - Query state, be prepared for the next query
    res[STATE_MACHINE__NEW_STATE] = QUERY_STATE

    return res

def parse_query_state_transition(parsed_msgs, output_msg, backend_db_con) :
    """! Performs parse query.
    @param 

    @return
    
    """
    logging.info("Entering parse_query_state_transition")

    # Initialization
    res = {}
    msg = bytes('', "utf-8")

    # Munch Parse message from input (input 'P', output '1'), and get query stinrg
    assert len(parsed_msgs) > 0, "Receied an empty input parsed messages"
    input_msg = parsed_msgs[0]
    parsed_msgs = parsed_msgs[1:]
    assert (input_msg[MSG_ID] == PARSE_MSG_ID), f"Received a wrong message ID {input_msg[MSG_ID]}"

    query = input_msg[PARSE_MSG__QUERY]

    logging.info ("Recieved query :\n" + (query.decode("utf-8")))
    msg += One_Msg_ParseComplete_Serialize()
    
    # Munch Bind message from input (input 'B', output '2') 
    assert len(parsed_msgs) > 0, "Receied an empty input parsed messages"
    input_msg = parsed_msgs[0]
    parsed_msgs = parsed_msgs[1:]
    assert (input_msg[MSG_ID] == BIND_MSG_ID), f"Received a wrong message ID {input_msg[MSG_ID]}"
    msg += Two_Msg_BindComplete_Serialize()

    # Munch Describe message from input (input 'D', output 'T') 
    assert len(parsed_msgs) > 0, "Receied an empty input parsed messages"
    input_msg = parsed_msgs[0]
    parsed_msgs = parsed_msgs[1:]
    assert (input_msg[MSG_ID] == DESCRIBE_MSG_ID), f"Received a wrong message ID {input_msg[MSG_ID]}"

    cols_desc = {}
    if is_pg_catalog_msg(query) :        
        logging.info ("Got initial PBI type query\n")         
        cols_desc  = prepare_pg_catalog_cols_desc(query)
        cols_value = prepare_pg_catalog_cols_value(query)

    msg += T_Msg_RowDescription_Serialize(cols_desc)

    # Munch Execution message from input (input 'E', output: a lot of 'D's) 
    assert len(parsed_msgs) > 0, "Receied an empty input parsed messages"
    input_msg = parsed_msgs[0]
    parsed_msgs = parsed_msgs[1:]
    assert (input_msg[MSG_ID] == EXECUTE_MSG_ID), f"Received a wrong message ID {input_msg[MSG_ID]}"


        # msg += D_Msg_DataRow_Serialize(cols_desc, cols_values) 

        # num_of_lines = len(result)
        # msg += C_Msg_CommandComplete_Serialize('SELECT ' + str(num_of_lines)) 

    # # Query backend database
    # result = execute_query(backend_db_con, query)

    # # Serialize Response    
    # send_msg = T_Msg_RowDescription_Serialize(['xint']) 

    # for row_val in result :
    #     send_msg += D_Msg_DataRow_Serialize(row_val) 


    # send_msg += C_Msg_CommandComplete_Serialize('SELECT 3') 
    # send_msg += Z_Msg_ReadyForQuery_Serialize(READY_FOR_QUERY_SERVER_STATUS_IDLE)

    # Next state - Query state, be prepared for the next query
    # new_state = QUERY_STATE

    res[STATE_MACHINE__PARSED_MSGS] = parsed_msgs
    res[STATE_MACHINE__OUTPUT_MSG] = output_msg + msg
    res[STATE_MACHINE__IS_TX_MSG] = False

    # Next state - Query state, be prepared for the next query
    res[STATE_MACHINE__NEW_STATE] = QUERY_STATE

    # TX Response
    # return (new_state, send_msg)
    return res


# ---------------------------------------------------------------------------------------------
HOST = "192.168.4.64"
PORT = 5000
DATABASE = "master"
CLUSTERED = False
USERNAME = "sqream"
PASSWORD = "sqream"

# Put it all together
def CreatePGStateMachine() :
    pg_mimic = PG_StateMachine()
    pg_mimic.add_state(STARTUP_STATE, startup_transition)
    pg_mimic.add_state(PASSWORD_STATE, password_state_transition)
    pg_mimic.add_state(PARAMETER_STATUS_STATE, patameter_status_state_transition)
    pg_mimic.add_state(QUERY_STATE, query_state_transition)
    pg_mimic.add_state(SIMPLE_QUERY_STATE, simple_query_state_transition)
    pg_mimic.add_state(PARSE_QUERY_STATE, parse_query_state_transition)
    pg_mimic.add_state(END_STATE, None, end_state=1)
    pg_mimic.set_start(STARTUP_STATE)

    pg_mimic.backend_db_con = get_db(   host = HOST, port = PORT, 
                                        database = DATABASE, 
                                        username = USERNAME, password = PASSWORD)

    return pg_mimic

def is_initial_state(sm) :
    """
    Input : Receives a  PG_StateMachine()
    Output : True if state is STARTUP_STATE, False otherwise.
    """
    return True if sm.new_state == STARTUP_STATE else False

def force_initial_state(sm) :
    """
    Forces the state machine to return to initial state, in case session ended
    Input : Receives a  PG_StateMachine()
    Output : N/A
    """
    sm.new_state = STARTUP_STATE