import pandas as pd  
import numpy as np  
import argparse
import matplotlib.pyplot as plt

def toInt(num):
    try:
        return int(num)
    except ValueError:
        return None
def toFloat(num):
    try:
        return float(num)
    except ValueError:
        return None

# Parse data interval  (default interval: 5 days)
parser = argparse.ArgumentParser()
data_timestamp = 1776
parser.add_argument("--timestamp", type=int, default=1776, help="Insert timestamp to get requests")
args = parser.parse_args()
if args.timestamp:
    timest = args.timestamp
    if toInt(timest) is not None:
        data_timestamp = int(timest)

dataset_path = "./content_popularity/timestamp_{}.csv".format(data_timestamp)
dataset_all = pd.read_csv(dataset_path,names=["counts","content_id"], sep=";")

sum_all_request = dataset_all["counts"].sum()
print(sum_all_request)

def sum_element(df, start, end):
    return df.iloc[start:end, [0]]["counts"].sum()

def classification(Prob,thres):
    if Prob/4 > thres:
        return 1 
    elif Prob/3 > thres:
        return 2
    elif Prob/2 > thres:
        return 3
    elif Prob > thres:
        return 4
    else:
        return 5
def colorize(Class, index):
    if Class == 1:
        return [1,1,1,1]
    elif Class == 2:
        if ((index % 4) == 0):
            return [1,1,1,0]
        elif ((index % 4) == 1):
            return [1,1,0,1]
        elif ((index % 4) == 2):
            return [1,0,1,1]
        else:
            return [0,1,1,1]
    elif Class == 3:
        if ((index % 6) == 0):
            return [1,1,0,0]
        elif ((index % 6) == 1):
            return [0,1,1,0]
        elif ((index % 6) == 2):
            return [0,0,1,1]
        elif ((index % 6) == 3):
            return [1,0,0,1]
        elif ((index % 6) == 4):
            return [1,0,1,0]
        else:
            return [0,1,0,1]
    elif Class == 4:
        if ((index % 4) == 0):
            return [0,0,0,1]
        elif ((index % 4) == 1):
            return [0,0,1,0]
        elif ((index % 4) == 2):
            return [0,1,0,0]
        else:
            return [1,0,0,0]
    else:
        return [0,0,0,0]
# init value

dataset_all["Prob"] = dataset_all["counts"]*1000/sum_all_request
threshold = 0.82
dataset_all["Class"] = dataset_all.apply(lambda row: classification(row.Prob, threshold) , axis=1) 
dataset_all["Index"] = range(0,len(dataset_all))
dataset_all["Color"] = dataset_all.apply(lambda row: colorize(row.Class, row.Index) , axis=1) 
dataset_all[["content_id","Color"]].to_csv("./result/colored_{}.csv".format(data_timestamp), header=False, sep=";", index=False)