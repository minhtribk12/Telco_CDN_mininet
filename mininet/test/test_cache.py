import pandas as pd  
import numpy as np 
from lfu_cache import *
import argparse
from ast import literal_eval

def checkColor(cache_num, color=[0, 0, 0, 0]):
    position = cache_num % 4
    color_tup = literal_eval(color)
    if (color_tup[position] == 1):
        return True
    else:
        return False

# Parse cache number
parser = argparse.ArgumentParser(description="Cache Server tests")

parser.add_argument('--number', '-n',
                    dest="cache_num",
                    type=int,
                    help="Number of cache.",
                    default=0)

# Export parameters
args = parser.parse_args()
cache_num = args.cache_num

# Create dataframe containing request with its color
request_path = "./cache/Cache_1.csv"
request = pd.read_csv(request_path,names=["content_id"], sep=";")

rank_path = "./rank/colored_1776.csv"
content_rank = pd.read_csv(rank_path,names=["content_id", "Color"], sep=";")

#print(content_rank)
df_join = pd.merge(request, content_rank, on="content_id", how="left")

# Create LFU Cache Object with capacity = 100
my_cache = LFUCache(100)

# Read one by one request
for index, rq in df_join.iterrows():
    if (my_cache.get(rq["content_id"]) == -1):
        if checkColor(cache_num, rq["Color"]):
            my_cache.set(rq["content_id"],rq["Color"])
print(my_cache)