import pandas as pd  
import numpy as np 
from lfu_cache import *
import socket, json, time, threading, SocketServer, argparse
from ast import literal_eval
from jsonsocket import Client, _send, _recv

# Parse cache number
parser = argparse.ArgumentParser(description="Cache Server")

parser.add_argument('--number', '-n',
                    dest="cache_num",
                    type=int,
                    help="Number of cache.",
                    default=0)
parser.add_argument('--timestamp', '-t',
                    dest="timestamp",
                    type=int,
                    help="Timestamp of dataset.",
                    default=1776)
# Export parameters
args = parser.parse_args()
cache_num = args.cache_num
time_dataset = args.timestamp

# Init lock to synchronize
lock_cache = threading.Lock()
lock_client = threading.Lock()
lock_request = threading.Lock()
lock_response = threading.Lock()

# Init some value for test
this_ip = "127.0.0.1"
if (cache_num == 0):
    this_port = 10000
elif (cache_num == 1):
    this_port = 10001
else:
    this_port = 10002

next_ip = "127.0.0.1"
if (cache_num == 0):
    next_port = 10001
elif (cache_num == 1):
    next_port = 10000

origin_ip = "127.0.0.1"
origin_port = 10002
hop_thres = 1

# Init client 
client = Client()

# Create dataframe containing request with its color
request_path = "./request/Cache_{}.csv".format(cache_num+1)
request = pd.read_csv(request_path,names=["content_id"], sep=";")

rank_path = "./rank/colored_{}.csv".format(time_dataset)
content_rank = pd.read_csv(rank_path,names=["content_id", "color"], sep=";")
df_request = pd.merge(request, content_rank, on="content_id", how="left")
df_request["is_request"] = 1
df_request["hop_count"] = 0
df_request["source_ip"] = "127.0.0.1"
df_request["source_port"] = 10000+cache_num



# Create LFU Cache Object with capacity = 100
color_cache = LFUCache(90)
normal_cache = LFUCache(10)

def checkColor(cache_num, color=[0, 0, 0, 0]):
    position = cache_num % 4
    color_tup = literal_eval(color)
    if (color_tup[position] == 1):
        return True
    else:
        return False
def init_cache(cache, df):
    for index, rq in df.iterrows():
        cache.set(rq["content_id"],rq["color"])

def not_in_cache(content_id):
    lock_cache.acquire()
    in_color_cache = color_cache.get(content_id)
    in_normol_cache = normal_cache.get(content_id)
    lock_cache.release()
    if ((in_color_cache == -1) & (in_normol_cache == -1)):
        return True
    else:
        return False
def send_request(data,des_ip,des_port,source_ip,source_port):
    client_soc = Client()
    data["source_ip"] = source_ip
    data["source_port"] = source_port
    #lock_client.acquire()
    client_soc.connect(des_ip, des_port).send(data)
    client_soc.close()
    #lock_client.release()
    

def send_response(data,des_ip,des_port,source_ip,source_port):
    client_soc = Client()
    #lock_client.acquire()
    client_soc.connect(des_ip, des_port).send(data)
    client_soc.close()
    #lock_client.release()
    return 0

# Init Cache
#init_cache(color_cache, df_request)
#init_cache(normal_cache, df_request)

class ThreadedTCPRequestHandler(SocketServer.BaseRequestHandler):

    def handle(self):
        if not self.request:
            raise Exception('You have to connect first before sending data')
        data = _recv(self.request)
        data["hop_count"] = data["hop_count"] + 1
        cur_thread = threading.current_thread()
        print("Receive data in thread {}".format(cur_thread.name))
        if (data["is_request"] == 1):
            if ((not_in_cache(data["content_id"])) & (cache_num != 2)):
                if (data["hop_count"] >= hop_thres):
                    send_request(data, origin_ip, origin_port, this_ip, this_port)
                else:
                    send_request(data, next_ip, next_ip, this_ip, this_port)
                lock_request.acquire()
                self.server.requested_table = self.server.requested_table.append({"is_request": data["is_request"],
                                                                                    "content_id": data["content_id"], 
                                                                                    "hop_count": data["hop_count"], 
                                                                                    "color": data["color"], 
                                                                                    "source_ip": data["source_ip"],
                                                                                    "source_port": data["source_port"]}, ignore_index=True)
                lock_request.release()
            else:
                data["is_request"] = 0
                temp_ip = data["source_ip"]
                temp_port = data["source_port"]
                data["source_ip"] = this_ip
                data["source_port"] = this_port
                send_response(data, temp_ip, temp_port, this_ip, this_port)
        else:
            lock_cache.acquire()
            color_cache.set(data["content_id"],data["color"])
            lock_cache.release()
            lock_request.acquire()
            response_list = self.server.requested_table[self.server.requested_table["content_id"] == data["content_id"]]
            self.server.requested_table = self.server.requested_table[self.server.requested_table["content_id"] != data["content_id"]]
            lock_request.release()
            for i in range(0, response_list.shape[0]):
                request_ = response_list.iloc[i]
                if ((request_["source_ip"] == this_ip) & (request_["source_port"] == this_port)):
                    lock_response.acquire()
                    self.server.responsed_table = self.server.responsed_table.append({"content_id": data["content_id"],
                                                                "hop_count": data["hop_count"]}, ignore_index=True)
                    lock_response.release()
                else:
                    temp_ip = request_["source_ip"]
                    temp_port = request_["source_port"]
                    data["source_ip"] = this_ip
                    data["source_port"] = this_port
                    send_response(data, temp_ip, temp_port, this_ip, this_port)

class ThreadedTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    pass




server = ThreadedTCPServer((this_ip, this_port), ThreadedTCPRequestHandler)
server.requested_table = pd.DataFrame(columns=["is_request", "content_id", "hop_count", "color", "source_ip", "source_port"])
server.responsed_table = pd.DataFrame(columns=["content_id", "hop_count"])

# Start a thread with the server -- that thread will then start one
# more thread for each request
server_thread = threading.Thread(target=server.serve_forever)
# Exit the server thread when the main thread terminates
server_thread.daemon = True
server_thread.start()
print(type(server_thread))
print "Server loop running in thread:", server_thread.name
time.sleep(2)

# Starting request content
if (cache_num != 2):
    for i in range(0,df_request.shape[0]):
        cur_request = df_request.iloc[i]
        if not_in_cache(cur_request["content_id"]):
            temp = cur_request.to_json(orient="index")
            df_json = json.loads(temp)
            send_request(df_json,next_ip,next_port,this_ip,this_port)
            lock_request.acquire()
            server.requested_table = server.requested_table.append({"is_request": df_json["is_request"],
                                                                    "content_id": df_json["content_id"], 
                                                                    "hop_count": df_json["hop_count"], 
                                                                    "color": df_json["color"], 
                                                                    "source_ip": this_ip,
                                                                    "source_port": this_port}, ignore_index=True)
            lock_request.release()
        else:
            lock_response.acquire()
            server.responsed_table = server.responsed_table.append({"content_id": cur_request["content_id"], 
                            "hop_count": cur_request["hop_count"]}, ignore_index=True)
            lock_response.release()
    server_thread.join(10.0)
    #print(server.requested_table) 
    print(server.responsed_table[server.responsed_table["hop_count"] > 0])
    print(color_cache)
    print(normal_cache)

while True:
    print("Waiting for connection")
    time.sleep(1)
server.shutdown()
server.server_close()
