import pandas as pd  
import numpy as np 
from lfu_cache import *
import socket, json, time, threading, SocketServer, argparse
from ast import literal_eval
from jsonsocket import Client, _send, _recv
import os, os.path

# Parse cache number
parser = argparse.ArgumentParser(description="Cache Server")

parser.add_argument('--id', '-i',
                    dest="cache_id",
                    type=int,
                    help="ID of cache.",
                    default=0)
parser.add_argument('--timestamp', '-t',
                    dest="timestamp",
                    type=int,
                    help="Timestamp of dataset.",
                    default=1776)
parser.add_argument('--cachetype', '-c',
                    dest="cachetype",
                    type=int,
                    help="Type of cache: 0 -> No cache, 1 -> Original LFU, 2 -> Color-based Cache",
                    default=2)
# Export parameters
args = parser.parse_args()
cache_id = args.cache_id
time_dataset = args.timestamp
cache_type = args.cachetype

# Init lock to synchronize
lock_log = threading.Lock()
lock_cache = threading.Lock()
lock_client = threading.Lock()
lock_request = threading.Lock()
lock_response = threading.Lock()

# Init some IP, port for test
request_ip_table = {
    "red": "127.0.0.1",
    "green": "127.0.0.1",
    "blue": "127.0.0.1",
    "yellow": "127.0.0.1",
    "default": "127.0.0.1"
}
request_port_table = {
    "red": 10000,
    "green": 10000,
    "blue": 10000,
    "yellow": 10000,
    "default": 10100
}

# Read IP table
ip_table_path = "~/workspace/telco_cdn_mininet/mininet/source/cache_algorithm/ip_table/ip_table.csv"
df_ip_table = pd.read_csv(ip_table_path,names=["cache_id", "this_ip", "this_port", "red_ip", "red_port", "green_ip", "green_port", "blue_ip", "blue_port", "yellow_ip", "yellow_port", "default_ip", "default_port"], sep=",")
this_ip_df = df_ip_table[df_ip_table["cache_id"] == cache_id]

this_ip = this_ip_df.iloc[0]["this_ip"]
this_port = this_ip_df.iloc[0]["this_port"]

request_ip_table["red"] = this_ip_df.iloc[0]["red_ip"]
request_ip_table["green"] = this_ip_df.iloc[0]["green_ip"]
request_ip_table["blue"] = this_ip_df.iloc[0]["blue_ip"]
request_ip_table["yellow"] = this_ip_df.iloc[0]["yellow_ip"]
request_ip_table["default"] = this_ip_df.iloc[0]["default_ip"]

request_port_table["red"] = this_ip_df.iloc[0]["red_port"]
request_port_table["green"] = this_ip_df.iloc[0]["green_port"]
request_port_table["blue"] = this_ip_df.iloc[0]["blue_port"]
request_port_table["yellow"] = this_ip_df.iloc[0]["yellow_port"]
request_port_table["default"] = this_ip_df.iloc[0]["default_port"]

# Create dataframe containing request with its color
request_path = "~/workspace/telco_cdn_mininet/mininet/source/cache_algorithm/request/Cache_{}.csv".format(cache_id+1)
request = pd.read_csv(request_path,names=["content_id"], sep=";")

# Read content rank & color
rank_path = "~/workspace/telco_cdn_mininet/mininet/source/cache_algorithm/rank/colored_{}.csv".format(time_dataset)
content_rank = pd.read_csv(rank_path,names=["content_id", "color"], sep=";")

# Create requests
df_request = pd.merge(request, content_rank, on="content_id", how="left")
df_request["is_request"] = 1
df_request["hop_count"] = 0
df_request["source_ip"] = this_ip
df_request["source_port"] = this_port
df_request["visited"] = df_request["color"]



# Create LFU Cache Object with capacity = 100 (should test LRU and compare)
color_cache = LFUCache(90)
normal_cache = LFUCache(10)


# Function check color of content, if it matches cache color return True, otherwise return False
# Cache ID mush depend on cache color ***
def checkColor(cache_id, color):
    position = cache_id % 4
    color_tup = literal_eval(color)
    if (color_tup[position] == 1):
        return True
    else:
        return False

# Warm-up cache with a datafame of requests
def warm_up_cache(cache, df):
    for index, rq in df.iterrows():
        cache.set(rq["content_id"],rq["color"])

# Check a content in cache, if it doesn't exist, return True
def not_in_cache(content_id):
    lock_cache.acquire()
    in_color_cache = color_cache.get(content_id)
    in_normol_cache = normal_cache.get(content_id)
    lock_cache.release()
    if ((in_color_cache == -1) & (in_normol_cache == -1)):
        return True
    else:
        return False

# Function send a content request to other server
def send_request(data,des_ip,des_port,source_ip,source_port):
    data["source_ip"] = source_ip
    data["source_port"] = source_port
    #lock_client.acquire()
    socket = None
    sent = False
    counter = 0
    while (counter < 5) & (sent == False):
        client_soc = Client()
        socket, success = client_soc.connect(des_ip, des_port)
        if (success):
            if socket != None:
                if(socket.send(data)):
                    sent = True
                    break
        counter += 1
        if (count == 5):
            lock_log.acquire()
            with open("log_{}.txt".format(cache_id), "a+") as logfile:
                logfile.write("Request {} can not be sent".format(data["content_id"]))
            lock_log.release()
            break
        time.sleep(1)
    if sent:
        client_soc.close()
    #lock_client.release()
    
# Function response a content to other server
def send_response(data,des_ip,des_port,source_ip,source_port):
    #lock_client.acquire()
    socket = None
    sent = False
    counter = 0
    while (counter < 5) & (sent == False):
        client_soc = Client()
        socket, success = client_soc.connect(des_ip, des_port)
        if (success):
            if socket != None:
                if(socket.send(data)):
                    sent = True
                    break
        counter += 1
        if (count == 5):
            lock_log.acquire()
            with open("log_{}.txt".format(cache_id), "a+") as logfile:
                logfile.write("Response {} can not be sent".format(data["content_id"]))
            lock_log.release()
            break
        time.sleep(1)
    if sent:
        client_soc.close()
    #lock_client.release()

def visit_cache(visited):
    visit_tup = literal_eval(visited)
    for i in range(0,4):
        if visit_tup[i] == 1:
            return i
    return -1

def find_destination(next_visit):
    if (next_visit == -1):
        return request_ip_table["default"], request_port_table["default"]
    elif (next_visit == 0):
        return request_ip_table["red"], request_port_table["red"]
    elif (next_visit == 1):
        return request_ip_table["green"], request_port_table["green"]
    elif (next_visit == 2):
        return request_ip_table["blue"], request_port_table["blue"]
    elif (next_visit == 3):
        return request_ip_table["yellow"], request_port_table["yellow"]

# Warm-up Cache
warm_up_cache(color_cache, df_request)
warm_up_cache(normal_cache, df_request)

class ThreadedTCPRequestHandler(SocketServer.BaseRequestHandler):
# Class handle connection form other server
# Each connection will be handled in a separated thread
    
    # Default function handle connection
    def handle(self):
        if not self.request:
            raise Exception('You have to connect first before sending data')
        # Receive data from connection
        data = _recv(self.request)
        # Increase hop count before process
        if cache_id == 100:
            data["hop_count"] = data["hop_count"] + 10
        else: 
            data["hop_count"] = data["hop_count"] + 1
        # Print out thread to debug
        cur_thread = threading.current_thread()
        print("Receive data in thread {}".format(cur_thread.name))

        if (data["is_request"] == 1):
            # This is a request
            if ((not_in_cache(data["content_id"])) & (cache_id != 100)):
                # Requested content doesn't exist in cache and this cache is not origin (should change when origin change)
                lock_request.acquire()
                # Regist a request to a table, which is used to response when this server receive the content from other servers 
                self.server.requested_table = self.server.requested_table.append({"is_request": data["is_request"],
                                                                                    "content_id": data["content_id"], 
                                                                                    "hop_count": data["hop_count"], 
                                                                                    "color": data["color"], 
                                                                                    "source_ip": data["source_ip"],
                                                                                    "source_port": data["source_port"]}, ignore_index=True)
                lock_request.release()
                # Update source IP and port before send the request to other server
                data["source_ip"] = this_ip
                data["source_port"] = this_port

                # Mark color visited
                position = cache_id % 4
                color_tup = literal_eval(data["visited"])
                color_tup[position] = 0
                data["visited"] = str(color_tup)

                # Find next Ip to send a request
                des_ip, des_port = find_destination(visit_cache(data["visited"]))

                send_request(data, des_ip, des_port, this_ip, this_port)
                # if (data["hop_count"] >= hop_thres):
                #     # This means the server which has the same color with the content also doesn't has requested content (should change when origin change)
                #     send_request(data, origin_ip, origin_port, this_ip, this_port)
                # else:
                #     # Send the request to adjacent server which has the same color with the requested content (should check server color to chose next IP,port)
                #     send_request(data, next_ip, next_port, this_ip, this_port)
                    
            else:
                # The requested content was found in cache
                # Set it to a response
                data["is_request"] = 0
                # Update source IP and port before sending the response
                temp_ip = data["source_ip"]
                temp_port = data["source_port"]
                data["source_ip"] = this_ip
                data["source_port"] = this_port
                send_response(data, temp_ip, temp_port, this_ip, this_port)
        else:
            # This is a response
            if checkColor(cache_id, data["color"]):
                lock_cache.acquire()
                # Set the content in to cache (should check color first)
                color_cache.set(data["content_id"],data["color"])
                lock_cache.release()
            else:
                lock_cache.acquire()
                # Set the content in to cache (should check color first)
                normal_cache.set(data["content_id"],data["color"])
                lock_cache.release()
            # Check response table to before fowarding content to servers who requested for this content
            lock_request.acquire()
            response_list = self.server.requested_table[self.server.requested_table["content_id"] == data["content_id"]]
            self.server.requested_table = self.server.requested_table[self.server.requested_table["content_id"] != data["content_id"]]
            lock_request.release()
            # Process one by one (should be parallelized)
            for i in range(0, response_list.shape[0]):
                request_ = response_list.iloc[i]
                if ((request_["source_ip"] == this_ip) & (request_["source_port"] == this_port)):
                    # This content was requested by this server's client
                    lock_response.acquire()
                    # update response table 
                    self.server.responsed_table = self.server.responsed_table.append({"content_id": data["content_id"],
                                                                "hop_count": data["hop_count"]}, ignore_index=True)
                    lock_response.release()
                else:
                    # This content was requested by other server
                    # Update destination ip, port before sending it
                    temp_ip = request_["source_ip"]
                    temp_port = request_["source_port"]
                    data["source_ip"] = this_ip
                    data["source_port"] = this_port
                    send_response(data, temp_ip, temp_port, this_ip, this_port)

class ThreadedTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
# Threading Server Class
# Use defaul init
    pass

###################################### Main script body ################################################################## 
# Create server object
server = ThreadedTCPServer((this_ip, this_port), ThreadedTCPRequestHandler)

# Init 2 table as server resources
server.requested_table = pd.DataFrame(columns=["is_request", "content_id", "hop_count", "color", "source_ip", "source_port"])
server.responsed_table = pd.DataFrame(columns=["content_id", "hop_count"])

# Start a thread with the server -- that thread will then start one more thread for each request
server_thread = threading.Thread(target=server.serve_forever)
# Exit the server thread when the main thread terminates
server_thread.daemon = True
server_thread.start()
print "Server loop running in thread:", server_thread.name
# Wait for all servers are init (should use signal)
time.sleep(2)

# Starting request content
if (cache_id != 100):
    # It's not a origin
    for i in range(0,df_request.shape[0]):
        # Send one by one 
        cur_request = df_request.iloc[i]
        if not_in_cache(cur_request["content_id"]):
            # Requested content doesn't exist in cache
            # Transform dataframe to json format
            temp = cur_request.to_json(orient="index")
            df_json = json.loads(temp)
            # Send request to next server has color matches it's color (should check color first)
            position = cache_id % 4
            color_tup = literal_eval(df_json["visited"])
            color_tup[position] = 0
            df_json["visited"] = str(color_tup)

            # Find next Ip to send a request
            des_ip, des_port = find_destination(visit_cache(df_json["visited"]))

            send_request(df_json,des_ip,des_port,this_ip,this_port)
            lock_request.acquire()
            # Update request table
            server.requested_table = server.requested_table.append({"is_request": df_json["is_request"],
                                                                    "content_id": df_json["content_id"], 
                                                                    "hop_count": df_json["hop_count"], 
                                                                    "color": df_json["color"], 
                                                                    "source_ip": this_ip,
                                                                    "source_port": this_port}, ignore_index=True)
            lock_request.release()
        else:
            # Requested content exists in cache
            lock_response.acquire()
            # Update responsed table
            server.responsed_table = server.responsed_table.append({"content_id": cur_request["content_id"], 
                                                                    "hop_count": cur_request["hop_count"]}, ignore_index=True)
            lock_response.release()
    server_thread.join(10.0)
    # Print results for debug
    df_result = pd.DataFrame(columns=["cache_id", "sum_hop"])
    sum_hop_count = server.responsed_table["hop_count"].sum()
    df_result = df_result.append({"cache_id": cache_id, 
                                    "sum_hop": sum_hop_count}, ignore_index=True)
    df_result.to_csv("~/workspace/telco_cdn_mininet/mininet/source/cache_algorithm/result/result_{}.csv".format(cache_id), header=False, sep=";", index=False)
    
    # print(server.responsed_table[server.responsed_table["hop_count"] == 0].count())
    # print(server.responsed_table[server.responsed_table["hop_count"] == 2].count())
    # print(server.responsed_table[server.responsed_table["hop_count"] == 4].count())

DIR = '~/workspace/telco_cdn_mininet/mininet/source/cache_algorithm'
while True:
    if (len([name for name in os.listdir(DIR) if os.path.isfile(os.path.join(DIR, name))]) >= 4):
        break
    print("Waiting for connection")
    time.sleep(1)
server.shutdown()
server.server_close()
