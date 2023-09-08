"""
### Hbench command aggregator

Usage:
nohup python3 batchRun.py -t 20 -c 100 -p 500 -i 1000 -o descending -l "8GBHeap_Pool10_instance1" -s "-H dbrscale-w5f1fv-gateway0.dbrscale.svbr-nqvp.int.cldr.work --savedata /tmp/benchdata --sanitize -M 'addPartition' -M 'getPartitions' -M 'getPartitionNames' -M 'dropPartitions' -M 'getTable'" &

Out:
multiple csv for each configuration
"""

import getopt
import logging
import os
import subprocess
import sys
import time

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def execute_run(cmdList):

    try:
        for cmd in cmdList:
            logger.info("running cmd - "+cmd)
            subprocess.Popen(["bin/hbench",cmd])
    except:
        logger.info("oops some problem injecting " + str(x))


def main(argv):
    threads = [1]
    cols = [1]
    partitions = [1]
    instances = [100]

    try:
        opts, args = getopt.getopt(argv, "ht:c:p:i:o:s:l:",
                                   ["help", "numThreads=", "numCols=", "numPart=", "numInstances=","order=","script=","capacitytag="])
    except getopt.GetoptError:
        logger.info(
            'invalid usage : hms_meta_partition.py -t <numThreads> -c <numCols> -p <numParts> -i <numInstances> -o <order> -s <script> -l <capacitytag>')
        sys.exit(2)

    if len(opts) == 0:
        logger.info(
            'invalid usage : hms_meta_partition.py -t <numThreads> -c <numCols> -p <numParts> -i <numInstances> -o <order> -s <script> -l <capacitytag>')
        sys.exit(2)

    print(opts)
    input = {}
    THREADS = "-T"
    COLUMNS = "--columns"
    INSTANCES = "-N"
    PARTITIONS = "--params"
    OUTPUT = "-o"
    DBNAME = "-d"
    ASCENDING = "ascending"
    DESCENDING = "descending"

    for opt, arg in opts:
        if opt == '-h':
            logger.info(
                'hms_meta_partition.py -t <numThreads> -c <numCols> -p <numParts> -i <numInstances> -o <order> -s <script> -l <capacitytag>')
            sys.exit()
        elif opt in ("-t", "--numThreads"):
            threadsString = arg
            threads = threadsString.split(",")
            input[THREADS] = threads
        elif opt in ("-c", "--numCols"):
            colsString = arg
            cols = colsString.split(",")
            input[COLUMNS] = cols
        elif opt in ("-p", "--numPart"):
            partitionsString = arg
            partitions = partitionsString.split(",")
            input[PARTITIONS] = partitions
        elif opt in ("-i", "--numInstances"):
            instancesString = arg
            instances = instancesString.split(",")
            input[INSTANCES] = instances
        elif opt in ("-o", "--order"):
            order = arg
        elif opt in ("-s", "--script"):
            script = arg
        elif opt in ("-l", "--capacitytag"):
            capacity_tag = arg

    out = []
    files = []
    for count in range(len(input.keys())):
        innerCount = 0
        for innerKey in input.keys():
            if innerCount == count:
                currList = input[innerKey]
                currKey = innerKey
                break
            innerCount = innerCount + 1

        if order == DESCENDING:
            currList = currList[::-1]

        for curr in currList:
            cmd = ""
            file = ""

            for key in input.keys():
                if key == INSTANCES:
                    break
                if key != currKey:
                    if order == ASCENDING:
                        cmd = cmd + key + " "+ input[key][0]+" "
                        file = file+ f"{input[key][0]}{key.split('-')[-1]}_"
                    elif order == DESCENDING:
                        cmd = cmd + key + " "+ input[key][-1]+" "
                        file = file+ f"{input[key][-1]}{key.split('-')[-1]}_"

                else:
                    cmd = cmd + currKey+" "+curr+" " 
                    file = file+ f"{curr}{currKey.split('-')[-1]}_"

            if cmd not in out and len(cmd) > 0:
                #print(cmd)
                out.append(cmd)
                files.append(file)

    cmd=""
    file=""
    if order == ASCENDING:
        for key in input.keys():
            if key != INSTANCES:
                cmd = cmd + key + " "+ input[key][-1]+" "
                file = file+ f"{input[key][-1]}{key.split('-')[-1]}_"
    elif order == DESCENDING:
        for key in input.keys():
            if key != INSTANCES:
                cmd = cmd + key + " "+ input[key][0]+" "
                file = file+ f"{input[key][0]}{key.split('-')[-1]}_"

    out.append(cmd)
    files.append(file)
    
    fullcmd = []
    for index in range(len(out)):
        #print(index)
        file = files[index]
        cmd = out[index]
        currInstances = input[INSTANCES]
        for curr in currInstances:
            cmd = cmd + INSTANCES+" "+curr+" "
            file = file + f"{curr}{INSTANCES.split('-')[-1]}"
        file = capacity_tag + "_" + file
        cmd = cmd + OUTPUT + " "+file+".csv" + " "+ DBNAME + " " + file
        cmd = script +" "+ cmd
        #print(cmd)
        if cmd not in fullcmd:
            fullcmd.append(cmd)

    execute_run(fullcmd)

if __name__ == "__main__":
    main(sys.argv[1:])
