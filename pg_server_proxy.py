#!/usr/bin/python3
"""
Postgres Server Proxy for SqreamDB
Follows the reuiqred protocol messages, and keeps a state to respond correctly.
Designed to work with only a single client at a time (multithresding the server was done to deal with 
PowerBI issue, where it did not close a session, and started a new one).

References :
------------
FSM :                   https://www.python-course.eu/finite_state_machine.php
Logging :               https://docs.python.org/3/howto/logging.html
                        https://realpython.com/python-logging/
Postgres data formats : https://www.postgresql.org/docs/12/protocol-message-formats.html       
Server example :        https://docs.python.org/3/library/socketserver.html   
"""

import logging
logging.basicConfig(level=logging.DEBUG)


# *****************************************************
# * PG server logic
# *****************************************************
from pg_statemachine import *

import threading
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

            cur_thread = threading.current_thread()

            logging.info("*** {} : Client Port {}".format(cur_thread.name, self.client_address[1]))
            logging.info(self.data)
            logging.info("New state : {}".format(self.server.pg_sm.new_state))

            # Received an empty message - This means end of communication
            if len(self.data) == 0 :
                logging.error("*** pg_server_proxy : Received zero length message. Exiting")
                force_initial_state(self.server.pg_sm)
                break
            # Received a Startup message at the middle of the session - Return to initial state
            is_startup_msg = is_init_message(self.data[0:1])
            if is_startup_msg: 
               force_initial_state(self.server.pg_sm)

            # Tokenize input bytes stream to discrete messages
            tokens = tokenization(self.data, is_startup_msg)

            # Parse messages to their attributes
            parsed_msgs = parse(tokens)

            # Initialize the result return object from the state machine 
            res = {}
            res[STATE_MACHINE__IS_TX_MSG]   = False
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

# Multithreading the Server, enabling a client to start a new session, without closing the first one.
# This is a behaviour seen with PowerBI, after the Table Preview stage during connection to the database.
class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    pass

def RunPGServer(host, port) :
    # Create the server, binding to localhost on port PG_PORT
    ThreadedTCPServer.allow_reuse_address = True
    with ThreadedTCPServer((host, port), MyPGHandler) as server:
        server.pg_sm = CreatePGStateMachine()

        # Activate the server; this will keep running until you
        # interrupt the program with Ctrl-C

        # Start a thread with the server -- that thread will then start one
        # more thread for each request
        server_thread = threading.Thread(target=server.serve_forever)

        server_thread.start()
        print("Server loop running in thread:", server_thread.name)

        server_thread.join()

# *****************************************************
# * Main Functionality
# *****************************************************
if __name__ == "__main__" :
    PG_PORT = 5432
    HOST, PORT = "localhost", PG_PORT
    RunPGServer(HOST, PORT)