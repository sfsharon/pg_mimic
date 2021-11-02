#!/usr/bin/python3
"""
Postgres Client for unit testing
Transmits (plays) client (psql or Power BI) pre-recorded messages sequentialy for unit testing the pg_server_proxy module   
TCP Client example taken from: https://gist.github.com/homoluctus/5ee21411dd89cebbb237b51ab56f0a4c
"""

import logging
logging.basicConfig(level=logging.DEBUG)

import socket

# Power BI sequence of messages
PBI_STARTUP_MSG_1 = b'\x00\x00\x00>\x00\x03\x00\x00user\x00postgres\x00client_encoding\x00UTF8\x00database\x00postgres\x00\x00'
PBI_PASSWORD_MSG_2 = b'p\x00\x00\x00(md5b400a301a6904ae12fc76a8fff168215\x00'
PBI_PBDES_MSG_3 = b"P\x00\x00\x00\xb6\x00select TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE\r\nfrom INFORMATION_SCHEMA.tables\r\nwhere TABLE_SCHEMA not in ('information_schema', 'pg_catalog')\r\norder by TABLE_SCHEMA, TABLE_NAME\x00\x00\x00B\x00\x00\x00\x0e\x00\x00\x00\x00\x00\x00\x00\x01\x00\x01D\x00\x00\x00\x06P\x00E\x00\x00\x00\t\x00\x00\x00\x00\x00S\x00\x00\x00\x04"
PBI_MSGS = [PBI_STARTUP_MSG_1, PBI_PASSWORD_MSG_2, PBI_PBDES_MSG_3]

# psql sequence of messages
# ---- TODO ----

def run_UT(host, port, msgs):
    RX_BUFF_SIZE = 4096

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((host, port))

        while True:
            for msg in msgs :
                sock.sendall(msg)

                print("[+] Sending to {}:{}".format(host, port))

                response = sock.recv(RX_BUFF_SIZE)

                if not response:
                    print("[-] Not Received")
                    break

                print("[+] Received", repr(response.decode('utf-8')))

if __name__ == "__main__" :
    PG_PORT = 5432
    HOST = "localhost"
    run_UT(HOST, PG_PORT, PBI_MSGS)