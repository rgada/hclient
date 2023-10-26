## usage
# python3 analyze/ResultAggregator.py --job_name st-jmeterts-g6kzc0 --timestamp 2023-10-26T13:23:23Z

import argparse
import json

# read all the csv files in the folder
import glob

# All files and directories ending with .txt and that don't begin with a dot:
csvfiles = (glob.glob("*.csv")) 
print("number of files found - "+str(len(csvfiles)))
print(csvfiles)

# read into pandas dataframe
# additionally add a new column for the category
import pandas as pd

li = []

# get the job_name
parser = argparse.ArgumentParser(description='job_name')
# parser.add_argument('-path', '--path', help='path of pem key',required=True)
parser.add_argument('-job_name', '--job_name', type=str,
                    required=True,
                    help='job_name for the run')

# parser.add_argument('-path', '--path', help='path of pem key',required=True)
parser.add_argument('-timestamp', '--timestamp', type=str,
                    required=True,
                    help='timestamp for the run')

args = parser.parse_args()
job_name = args.job_name
timestamp = args.timestamp

for csvfile in csvfiles:
    df = pd.read_csv(csvfile,delimiter='\t')
    if "#" in csvfile:
        csvfile = csvfile.split("#")[0]
        
    df['Tag'] = pd.Series([csvfile] * len(df.index))
    df['job_name'] = pd.Series([job_name] * len(df.index))
    df['@timestamp'] = pd.Series([timestamp] * len(df.index))
       
    li.append(df)

# now concatenate the list
df = pd.concat(li,axis=0, ignore_index=True)

# rename for proper indexing in elasticsearch
df = df.rename(columns={'Operation': 'Opr', 'ResponseTime': 'RspTime', 'Tag': 'Label'})

# convert to master csv to be sent to elastic
df.to_csv("master.csv", index=False)
