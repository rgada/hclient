## usage
# python3 analyze/ResultAggregator.py --runId temp1

import argparse

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

# get the runid
parser = argparse.ArgumentParser(description='RunID')
# parser.add_argument('-path', '--path', help='path of pem key',required=True)
parser.add_argument('-runId', '--runId', type=str,
                    required=True,
                    help='RunID for the run')
args = parser.parse_args()
runId = args.runId

for csvfile in csvfiles:
    df = pd.read_csv(csvfile,delimiter='\t')
    if "#" in csvfile:
        csvfile = csvfile.split("#")[0]
        
    df['Tag'] = pd.Series([csvfile] * len(df.index))
    df['RunId'] = pd.Series([runId] * len(df.index))
    # print(df.head())
    li.append(df)

df = pd.concat(li,axis=0, ignore_index=True)
# print(df.head())

df.to_csv("master.csv")
df.to_json("master.json")