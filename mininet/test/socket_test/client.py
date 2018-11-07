from jsonsocket import Client
import time

host = 'LOCALHOST'
port = 10000

i=1
while True:
    client = Client()
    data = {
            "Request": 1,
            "Content_id": 318,
            "Hop_count": i
        }
    client.connect(host, port).send(data)
    i+=1
    print(data)
    response = client.recv()
    print(response)
    client.close()
    time.sleep(1)