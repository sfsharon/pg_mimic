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
#  TODO 27/10/2021 2130
  1. Integrate with PowerBI :
    Complete function parse(tokenized_msgs) :
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


            # Run state machine as long as the state transitions have nothing to transmit
            send_msg = ""
            while send_msg == "" :
                send_msg, parsed_msgs = self.server.pg_sm.run(parsed_msgs)

            # TX Response
            self.request.sendall(send_msg)


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