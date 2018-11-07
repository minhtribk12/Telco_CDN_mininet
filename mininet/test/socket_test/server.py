#file:server.py
from jsonsocket import Server

host = 'LOCALHOST'
port = 10000

server = Server(host, port)

while True:
    server.accept()
    data = server.recv()
    server.send(data)

server.close()