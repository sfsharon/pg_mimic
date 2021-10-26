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
#  TODO 18/10/2021 2130
  1. Implement SQream backend connection, and integrate query sequence.
  2. Integrate with PowerBI
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
                break

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
