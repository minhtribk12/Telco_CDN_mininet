import os
import tempfile
import pandas as pd
import argparse
import glob

# Load a text file to RDD and convert each line to a Row.

filepath = glob.glob("/home/hpcc/workspace/telco_cdn_mininet/mininet/source/cache_algorithm/result/*.csv")
df_result = pd.DataFrame(columns=["cache_id", "sum_hop"])
for result in filepath:
    df_temp = pd.read_csv(result,names=["cache_id", "sum_hop"], sep=";")
    df_result = pd.concat([df_result, df_temp], ignore_index=True)
print(df_result["sum_hop"].sum())


