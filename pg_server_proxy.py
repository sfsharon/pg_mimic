#!/usr/bin/python3
"""
Postgres Server Proxy
Follows the reuiqred protocol messages, and keeps a state to respond corretly.
FSM : https://www.python-course.eu/finite_state_machine.php
Logging : https://docs.python.org/3/howto/logging.html
          https://realpython.com/python-logging/
Postgres data formats : https://www.postgresql.org/docs/12/protocol-message-formats.html          
"""

import logging
logging.basicConfig(level=logging.DEBUG)

"""##################3
#  TODO 24/11/2021 0800
  1. Verify psql client still works
  2. PBI Messages : 
    2.1 Connection of PBI shows a query with variables (e.g, $Table)
        Need to replace variables with actual parameters before sending to SQream the query.
"""


# *****************************************************
# * PG server logic
# *****************************************************
from pg_statemachine import *

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

            if len(self.data) == 0 :
                logging.error("*** pg_server_proxy : Received zero length message. Exiting")
                force_initial_state(self.server.pg_sm)
                break

            # Tokenize input bytes stream to discrete messages
            is_expecting_startup_msg = is_initial_state(self.server.pg_sm)
            tokens = tokenization(self.data, is_expecting_startup_msg)

            # Parse messages to their attributes
            parsed_msgs = parse(tokens, is_expecting_startup_msg)

            # Initialize the result return object from the state machine 
            res = {}
            res[STATE_MACHINE__IS_TX_MSG] = False
            res[STATE_MACHINE__OUTPUT_MSG]  = bytes('', "utf-8")
            res[STATE_MACHINE__PARSED_MSGS] = parsed_msgs

            # As long as there are messages received from client that were not munched, keep processing them
            while len(res[STATE_MACHINE__PARSED_MSGS]) > 0: 
                # Run state machine as long as the state transitions have nothing to transmit
                while res[STATE_MACHINE__IS_TX_MSG] == False : 
                    res = self.server.pg_sm.run(res[STATE_MACHINE__PARSED_MSGS], 
                                                res[STATE_MACHINE__OUTPUT_MSG])

                # TX Response
                self.request.sendall(res[STATE_MACHINE__OUTPUT_MSG])
                # Enable running the state machine, if there are additional messages in the parsed_msgs
                res[STATE_MACHINE__IS_TX_MSG] = False
                res[STATE_MACHINE__OUTPUT_MSG]  = bytes('', "utf-8")

def RunPGServer(host, port) :
    # Create the server, binding to localhost on port PG_PORT
    socketserver.TCPServer.allow_reuse_address = True
    with socketserver.TCPServer((host, port), MyPGHandler) as server:
        server.pg_sm = CreatePGStateMachine()

        # Activate the server; this will keep running until you
        # interrupt the program with Ctrl-C
        logging.info("Starting PG proxy server")
        server.serve_forever()        


# *****************************************************
# * Main Functionality
# *****************************************************
if __name__ == "__main__" :
    PG_PORT = 5432
    HOST, PORT = "localhost", PG_PORT
    RunPGServer(HOST, PORT)