#!/usr/bin/python3
"""
Main module for Postgres to SQream connector.
Implements TCP server with state machine to handle incoming meesages from Postgres front end (e.g. psql client)
Socket server code based on the example in https://docs.python.org/3/library/socketserver.html
"""

import socketserver


class MyPGHandler(socketserver.BaseRequestHandler):
    """
    The request handler class for Postgres mimic server.
    """
    INPUT_BUFF_SIZE = 1024 * 1024

    def handle(self):
        # self.request is the TCP socket connected to the client
        self.data = self.request.recv(self.INPUT_BUFF_SIZE)
        print("{} wrote:".format(self.client_address[0]))
        print(self.data)
        # just send back the same data, but upper-cased
        self.request.sendall(self.data.upper())

if __name__ == "__main__":
    PG_PORT = 5432
    HOST, PORT = "localhost", PG_PORT

    # Create the server, binding to localhost on port PG_PORT
    with socketserver.TCPServer((HOST, PORT), MyPGHandler) as server:
        # Activate the server; this will keep running until you
        # interrupt the program with Ctrl-C
        server.serve_forever()