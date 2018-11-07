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
counts_101 = dataset_all.iloc[100].counts
sum_head_101 = dataset_all["counts"].head(101).sum()
num_request = sum_head_101 / counts_101

def sum_element(df, start, end):
    return df.iloc[start:end, [0]]["counts"].sum()

# def cal_num_request(idx):
#     if idx <= 100:
#         return sum_head_101 / counts_101
#     else:
#         return (sum_head_101 - counts_101) / counts_100

def cal_hit_request(counts,prob):
    return sum_all_request*prob*(1-((1-prob)**(num_request+1)))

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

dataset_all = dataset_all[dataset_all["counts"] > 0]
dataset_all["prob"] = dataset_all["counts"]/sum_all_request
#dataset_all["idx"] = range(0,len(dataset_all))
dataset_all["hit_request"] = dataset_all.apply(lambda row: cal_hit_request(row.counts, row.prob) , axis=1) 
print(dataset_all)


sum_hit = 0
remain_slot = 400.0
total_slot = 400.0
rate = 0.0
b = 100
c = 250
sum_max = 0
a_max = 0
found_a = False
for a in range (0,50):
    d = 400 - a - b - c
    if found_a:
        break
    for index, row in dataset_all.iterrows():
        remain = remain_slot
        if index < a:
            rate = 1.0
            remain_slot = remain_slot - 4.0
        elif index < a+b:
            rate = 0.75
            remain_slot = remain_slot - (3.0*0.75)
        elif index < a+b+c:
            rate = 0.5
            remain_slot = remain_slot - (2.0*0.5)
        elif index < a+b+c+d:
            rate = 0.25
            remain_slot = remain_slot - (1.0*0.25)
        sum_hit = sum_hit + rate*remain*row["hit_request"]/total_slot
        if remain_slot <= 0:
            if (sum_hit > sum_max):
                sum_max = sum_hit
                a_max = a
            if (sum_hit < sum_max):
                found_a = True
                print("A: %d"%a_max)
                print("Sum in A: %f"%sum_hit)
            sum_hit = 0
            remain_slot = 400
            break

a = a_max
b_max = 0
sum_max = 0
found_b = False
for b in range (a_max,100):
    d = 400 - a - b - c
    if found_b:
        break
    for index, row in dataset_all.iterrows():
        remain = remain_slot
        if index < a:
            rate = 1.0
            remain_slot = remain_slot - 4.0
        elif index < a+b:
            rate = 0.75
            remain_slot = remain_slot - (3.0*0.75)
        elif index < a+b+c:
            rate = 0.5
            remain_slot = remain_slot - (2.0*0.5)
        elif index < a+b+c+d:
            rate = 0.25
            remain_slot = remain_slot - (1.0*0.25)
        sum_hit = sum_hit + rate*remain*row["hit_request"]/total_slot
        if remain_slot <= 0:
            if (sum_hit > sum_max):
                sum_max = sum_hit
                b_max = b
            if (sum_hit < sum_max):
                found_b = True
                print("B: %d"%b_max)
                print("Sum in B: %f"%sum_hit)
            sum_hit = 0
            remain_slot = 400
            break

a = a_max
b = b_max
c_max = 0
sum_max = 0
found_c = False
for c in range (b_max,150):
    d = 400 - a - b - c
    if found_c:
        break
    for index, row in dataset_all.iterrows():
        remain = remain_slot
        if index < a:
            rate = 1.0
            remain_slot = remain_slot - 4.0
        elif index < a+b:
            rate = 0.75
            remain_slot = remain_slot - (3.0*0.75)
        elif index < a+b+c:
            rate = 0.5
            remain_slot = remain_slot - (2.0*0.5)
        elif index < a+b+c+d:
            rate = 0.25
            remain_slot = remain_slot - (1.0*0.25)
        sum_hit = sum_hit + rate*remain*row["hit_request"]/total_slot
        if remain_slot <= 0:
            if (sum_hit > sum_max):
                sum_max = sum_hit
                c_max = c
            if (sum_hit < sum_max):
                found_c = True
                print("C: %d"%c_max)
                print("Sum in C: %f"%sum_hit)
            sum_hit = 0
            remain_slot = 400
            break
d_max = 400 - a_max - b_max - c_max
print("D: %d"%d_max)