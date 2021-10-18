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

    def add_state(self, name, handler, end_state=0):
        name = name.upper()
        self.handlers[name] = handler

    def set_start(self, name):
        self.new_state = name.upper()

    def run(self, cargo):
        try:
            handler = self.handlers[self.new_state.upper()]
        except:
            raise InitializationError("must call .set_start() before .run()")

   
        (self.new_state, cargo) = handler(cargo)

        handler = self.handlers[self.new_state.upper()] 

        return cargo

# ------------------------------------------------------------------------------------

from pg_serdes import *

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
    """! Builds parameter status message during intialization phase.
         Does not take into account the password (the txt parameter)
    @param txt password string

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

    send_msg += Z_Msg_ReadyForQuery_Serialize()

    # Next state
    new_state = STARTUP_STATE

    # TX Response
    return (new_state, send_msg)

# ---------------------------------------------------------------------------------------------

# Put it all together
def CreatePGStateMachine() :
    pg_mimic = StateMachine()
    pg_mimic.add_state(STARTUP_STATE, startup_transition)
    pg_mimic.add_state(PASSWORD_STATE, password_state_transition)
    pg_mimic.add_state(PARAMETER_STATUS_STATE, patameter_status_state_transition)
    pg_mimic.add_state(END_STATE, None, end_state=1)
    pg_mimic.set_start(STARTUP_STATE)

    return pg_mimic