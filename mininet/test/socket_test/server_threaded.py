from jsonsocket import *

if __name__ == "__main__":
    # Init Server Port (0 means to select an arbitrary unused port)
    HOST, PORT = "localhost", 10000

    server = ThreadedTCPServer((HOST, PORT), ThreadedTCPRequestHandler)
    ip, port = server.server_address

    # Start a thread with the server -- that thread will then start one
    # more thread for each request
    server_thread = threading.Thread(target=server.serve_forever)
    # Exit the server thread when the main thread terminates
    server_thread.daemon = True
    server_thread.start()
    print "Server loop running in thread:", server_thread.name
    while True:
        print("Waiting for connection")
        time.sleep(1)

    server.shutdown()
    server.server_close()