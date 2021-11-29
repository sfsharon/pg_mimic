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
STATE_MACHINE__NEW_STATE   = "new_state"
STATE_MACHINE__OUTPUT_MSG  = "send_msg"
STATE_MACHINE__IS_TX_MSG   = "is_tx_msg"
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

def init_param_state_transition(parsed_msgs, output_msg, backend_db_con) :
    """! Builds parameter status message during intialization phase.
         Does not take into account the password (the msg parameter)
    @param msg password 

    @return parameter status message
    
    """
    logging.info("Entering init_param_state_transition")

    res = {}

    res[STATE_MACHINE__PARSED_MSGS] = parsed_msgs

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
        # *** Munch Sync message from input, output Ready for Query message (input 'S', output 'Z')        
        input_msg = parsed_msgs[0]
        parsed_msgs = parsed_msgs[1:]
        res[STATE_MACHINE__PARSED_MSGS] = parsed_msgs
        output_msg += Z_Msg_ReadyForQuery_Serialize(READY_FOR_QUERY_SERVER_STATUS_IDLE)
        is_tx_msg = True
        assert len(parsed_msgs) == 0, "Error - Receied additional message after the Sync message"
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

    is_DISCARD_ALL_msg = True if query == PG_DISCARD_ALL_QUERY else False

    if is_DISCARD_ALL_msg :
        # Do nothing for 'DISCAR ALL' query
        msg =  S_Msg_ParameterStatus_Serialize (str.encode('is_superuser'), str.encode('on'))
        msg += S_Msg_ParameterStatus_Serialize (str.encode('session_authorization'), str.encode('postgres'))
        msg += C_Msg_CommandComplete_Serialize(PG_DISCARD_ALL_STRING) 
        msg += Z_Msg_ReadyForQuery_Serialize(READY_FOR_QUERY_SERVER_STATUS_IDLE)
    else :  # Regular Query
        # Query backend database
        query_output = execute_query(backend_db_con, query.decode('utf-8'))
        cols_desc   = prepare_cols_desc(query_output[BACKEND_QUERY__DESCRIPTION][BACKEND_QUERY__DESC_COLS_NAME],
                                        query_output[BACKEND_QUERY__DESCRIPTION][BACKEND_QUERY__DESC_COLS_TYPE],
                                        query_output[BACKEND_QUERY__DESCRIPTION][BACKEND_QUERY__DESC_COLS_LENGTH],
                                        query_output[BACKEND_QUERY__DESCRIPTION][BACKEND_QUERY__DESC_COLS_FORMAT])
        cols_values = query_output[BACKEND_QUERY__RESULT]

        # Serialize Response
        msg = T_Msg_RowDescription_Serialize(cols_desc) 

        for col_values in cols_values :
            msg += D_Msg_DataRow_Serialize(cols_desc, col_values) 

        num_of_lines = len(cols_values)

        msg += C_Msg_CommandComplete_Serialize('SELECT ' + str(num_of_lines)) 

        msg += Z_Msg_ReadyForQuery_Serialize(READY_FOR_QUERY_SERVER_STATUS_IDLE)


    res[STATE_MACHINE__IS_TX_MSG] = True
    res[STATE_MACHINE__OUTPUT_MSG] = output_msg + msg
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
    is_tx_msg = False

    # *** Munch Parse message from input, output parse complete message (input 'P', output '1')
    assert len(parsed_msgs) > 0, "Receied an empty input parsed messages"
    input_msg = parsed_msgs[0]
    parsed_msgs = parsed_msgs[1:]
    assert (input_msg[MSG_ID] == PARSE_MSG_ID), f"Received a wrong message ID {input_msg[MSG_ID]}"

    # Get query stinrg
    query = input_msg[PARSE_MSG__QUERY]
    is_catalog_query = is_pg_catalog_msg(query)
    
    msg += One_Msg_ParseComplete_Serialize()
    
    # ***  Munch Bind message from input, output bind complete message (input 'B', output '2') 
    assert len(parsed_msgs) > 0, "Receied an empty input parsed messages"
    input_msg = parsed_msgs[0]
    parsed_msgs = parsed_msgs[1:]
    assert (input_msg[MSG_ID] == BIND_MSG_ID), f"Received a wrong message ID {input_msg[MSG_ID]}"
    msg += Two_Msg_BindComplete_Serialize()

    # ***  Munch Describe message from input, output row description message (input 'D', output 'T') 
    assert len(parsed_msgs) > 0, "Receied an empty input parsed messages"
    input_msg = parsed_msgs[0]
    parsed_msgs = parsed_msgs[1:]
    assert (input_msg[MSG_ID] == DESCRIBE_MSG_ID), f"Received a wrong message ID {input_msg[MSG_ID]}"

    cols_desc = {}
    if is_catalog_query:        
        # logging.info ("Got initial PBI type query\n")         
        cols_desc   = prepare_pg_catalog_cols_desc(query)
        cols_values = prepare_pg_catalog_cols_value(backend_db_con, query)
    else : 
        # Regular query
        query = query.decode("utf-8")
        logging.info ("Recieved query :\n" + (query))
        # Substitue variables to actual parameters in the SQL query
        query = remove_table_varable_from_query(query)
        # Query backend database
        query_output = execute_query(backend_db_con, query)
        cols_desc   = prepare_cols_desc(query_output[BACKEND_QUERY__DESCRIPTION][BACKEND_QUERY__DESC_COLS_NAME],
                                        query_output[BACKEND_QUERY__DESCRIPTION][BACKEND_QUERY__DESC_COLS_TYPE],
                                        query_output[BACKEND_QUERY__DESCRIPTION][BACKEND_QUERY__DESC_COLS_LENGTH],
                                        query_output[BACKEND_QUERY__DESCRIPTION][BACKEND_QUERY__DESC_COLS_FORMAT])
        cols_values  = query_output[BACKEND_QUERY__RESULT]

    msg += T_Msg_RowDescription_Serialize(cols_desc)

    # ***  Munch Execution message from input, output Data messages (input 'E', output: a lot of 'D's) 
    assert len(parsed_msgs) > 0, "Receied an empty input parsed messages"
    input_msg = parsed_msgs[0]
    parsed_msgs = parsed_msgs[1:]
    assert (input_msg[MSG_ID] == EXECUTE_MSG_ID), f"Received a wrong message ID {input_msg[MSG_ID]}"

    for col_values in cols_values :
        msg += D_Msg_DataRow_Serialize(cols_desc, col_values) 

    #  ***  Prepare command complete message
    num_of_lines = len(cols_values)
    msg += C_Msg_CommandComplete_Serialize('SELECT ' + str(num_of_lines)) 

    # *** Prepare ready for query message, if finished munching the input message
    if len(parsed_msgs) == 0 :
        msg += Z_Msg_ReadyForQuery_Serialize(READY_FOR_QUERY_SERVER_STATUS_IDLE)
        is_tx_msg = True
        
    res[STATE_MACHINE__PARSED_MSGS] = parsed_msgs
    res[STATE_MACHINE__OUTPUT_MSG]  = output_msg + msg
    res[STATE_MACHINE__IS_TX_MSG]   = is_tx_msg

    # Next state - Query state, be prepared for the next query
    res[STATE_MACHINE__NEW_STATE] = QUERY_STATE

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
    pg_mimic.add_state(PARAMETER_STATUS_STATE, init_param_state_transition)
    pg_mimic.add_state(QUERY_STATE, query_state_transition)
    pg_mimic.add_state(SIMPLE_QUERY_STATE, simple_query_state_transition)
    pg_mimic.add_state(PARSE_QUERY_STATE, parse_query_state_transition)
    pg_mimic.add_state(END_STATE, None, end_state=1)
    pg_mimic.set_start(STARTUP_STATE)

    pg_mimic.backend_db_con = get_db(   host = HOST, port = PORT, 
                                        database = DATABASE, 
                                        username = USERNAME, password = PASSWORD)

    return pg_mimic

def is_init_statemachine(sm, parsed_msgs) :
    """
    Input : Receives a  PG_StateMachine() and parsed input messages.
            If Startup message exists, it is always the first message.
    Output : True if state is STARTUP_STATE or f identified first message as Startup message, False otherwise.
    """
    is_init_state  = True if sm.new_state == STARTUP_STATE else False
    is_startup_msg = len(parsed_msgs) > 0 and parsed_msg[0][MSG_ID] == STARTUP_MSG_ID 

    return is_init_state or is_startup_msg 

def force_initial_state(sm) :
    """
    Forces the state machine to return to initial state, in case session ended
    Input : Receives a  PG_StateMachine()
    Output : N/A
    """
    sm.new_state = STARTUP_STATE