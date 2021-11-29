# pg_mimic
Postgres Server Proxy for SqreamDB
Follows the reuiqred Postgres protocol messages, and keeps a state to respond corretly.
Designed to work with only a single client at a time (multithresding the server was done to deal with 
PowerBI issue, where it did not close a session, and started a new one).

References :
------------
FSM :                   https://www.python-course.eu/finite_state_machine.php
Logging :               https://docs.python.org/3/howto/logging.html
                        https://realpython.com/python-logging/
Postgres data formats : https://www.postgresql.org/docs/12/protocol-message-formats.html       
Server example :        https://docs.python.org/3/library/socketserver.html 

