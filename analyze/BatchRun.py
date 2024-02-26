"""
### Hbench command aggregator

Usage:
nohup python3 analyze/BatchRun.py -t 5,10,20 -c 102,502,1002 -p 100,500,1000 -i 100,1000 -o descending -l "8GBHeap_Pool10_instance1" -s "-H dbrscale-w5f1fv-gateway0.dbrscale.svbr-nqvp.int.cldr.work --savedata /tmp/benchdata --sanitize -CR -M 'addPartition' -M 'getPartitions' -M 'getPartitionNames' -M 'dropPartitions' -M 'getTable'" &

nohup python3 analyze/BatchRun.py -u dbrscale-w5f1fv-gateway0.dbrscale.svbr-nqvp.int.cldr.work  -r a -t 5,10,20 -c 100,500,1000 -p 100,500,1000 -i 100 -o descending -l "8GBH_20P_01I_AddPartitions" -s "--savedata /tmp/benchdata --sanitize -CR -M 'addPartition.*'" &

python3 analyze/BatchRun.py -u dbrscale-w5f1fv-gateway0.dbrscale.svbr-nqvp.int.cldr.work  -r a -t 20 -c 1000 -p 1000 -i 100 -o descending -l "8GBH_20P_01I_AddPartitions" -s "--savedata /tmp/benchdata --sanitize -CR -M 'addPartition.*'"

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

DEFAULT_SPIN = 100
SPIN_COUNT = "--spin"
COMMAND_DELIMITER = ";"

THREADS = "--threads"
APIS = "-M"
COLUMNS = "--columns"
INSTANCES = "-N"
PARTITIONS = "--params"
OUTPUT = "-o"
DBNAME = "-d"
HOST="-H"
ASCENDING = "ascending"
DESCENDING = "descending"
MAX = "max"

def execute_run(cmdList, host_count, curr_dir):

    try:
        for cmd in cmdList:
            cmdsplits = cmd.split(COMMAND_DELIMITER)

            currinstance = 0
            spincount = int(float(DEFAULT_SPIN / host_count))
            processes = []
            for currcmd in cmdsplits:
                #reduce the number of threads inversely to number of hosts
                #extract threads and distribute per host
                threadsplit = currcmd.split(THREADS)
                firsthalf = threadsplit[0]
                distributed_threads = int(int(threadsplit[1].split(" ")[1])/(len(cmdsplits)))
                secondhalf = ' '.join(threadsplit[1].split(" ")[2:])
                currcmd = firsthalf + secondhalf

                currcmd = currcmd + " " + SPIN_COUNT + " " + str(spincount) + " " + THREADS + " "+ str(distributed_threads)
                logger.info("triggering background for cmd - "+currcmd)
                pid = subprocess.Popen([curr_dir+"/bin/hbench",currcmd])
                processes.append(pid)
                currinstance = currinstance + 1
            exit_codes = [p.wait() for p in processes]
            logger.info("done with all the processes")
            logger.info(exit_codes)
    except:
        logger.info("oops some problem injecting " + str(x))


def main(argv):
    threads = [1]
    cols = [1]
    partitions = [1]
    instances = [100]

    try:
        opts, args = getopt.getopt(argv, "ht:c:p:i:o:s:l:r:u:k:d:",
                                   ["help", "numThreads=", "numCols=", "numPart=", "numInstances=","order=","script=","capacitytag=","runtag=","hosturi=","krandom=","dir="])
    except getopt.GetoptError:
        logger.info(
            'invalid usage : hms_meta_partition.py -t <numThreads> -c <numCols> -p <numParts> -i <numInstances> -o <order> -s <script> -l <capacitytag> -r <runtag> -u <hosturi> -k <randomness> -d <curr_directory>')
        sys.exit(2)

    if len(opts) == 0:
        logger.info(
            'invalid usage : hms_meta_partition.py -t <numThreads> -c <numCols> -p <numParts> -i <numInstances> -o <order> -s <script> -l <capacitytag> -r <runtag> -u <hosturi> -k <randomness> -d <curr_directory>')
        sys.exit(2)

    print(opts)
    input = {}

    for opt, arg in opts:
        if opt == '-h':
            logger.info(
                'hms_meta_partition.py -t <numThreads> -c <numCols> -p <numParts> -i <numInstances> -o <order> -s <script> -l <capacitytag> -r <runtag> -u <hosturi> -k <randomness> -d <curr_directory>')
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
        elif opt in ("-r", "--run"):
            run_tag = arg
        elif opt in ("-u", "--hosturi"):
            hosts = arg
        elif opt in ("-k", "--krandom"):
            random = arg == 'true'
        elif opt in ("-d", "--dir"):
            curr_dir = arg

    original_order = order
    if order == MAX:
        order = ASCENDING

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

    if cmd not in out:
        out.append(cmd)
    files.append(file)

    # for c in out:
    #     print(c)

    fullcmd = []
    for index in range(len(out)):
        #print(index)
        file = files[index]
        cmd = out[index]
        currInstances = input[INSTANCES]
        for curr in currInstances:
            cmd = cmd + INSTANCES+" "+curr+" "
            file = file + f"{curr}{INSTANCES.split('-')[-1]}"
        file = "Run"+run_tag+str(index)+"_"+file + "_" + capacity_tag

        cmd_base = cmd

        # in case of random concurrent run
        if random:
            # we have multiple hosts, run in parallel and generate comma seperate commands
            hostsplits = hosts.split(',')
            apisplits = script.split(APIS)
            curr_script = apisplits[0] # set the first half of command
            apis = apisplits[1:]
            hostinstance = 0
            cmd = ""
            cmd_instance = 0

            for host in hostsplits:
                for api in apis:
                    file_name = file + "#" + str(cmd_instance)

                    curr = cmd_base + " " +OUTPUT + " "+file_name+".csv" + " " + DBNAME + " " + file_name
                    
                    curr = HOST + " " + host + " " + curr_script + APIS + api+ " " + curr

                    # add delimiter
                    if (cmd_instance < ((len(hostsplits) * len(apis)) - 1)):
                        cmd = cmd + curr + COMMAND_DELIMITER
                    else:
                        cmd = cmd + curr

                    cmd_instance = cmd_instance + 1
        else:
            # we have multiple hosts, run in parallel and generate comma seperate commands
            hostsplits = hosts.split(',')
            
            hostinstance = 0
            cmd = ""
            for host in hostsplits:
                file_name = file + "#" + str(hostinstance)

                curr = cmd_base + " " +OUTPUT + " "+file_name+".csv" + " " + DBNAME + " " + file_name
                
                curr = HOST + " " + host + " " + script +" "+ curr

                # add delimiter
                if (hostinstance < (len(hostsplits) - 1)):
                    cmd = cmd + curr + ";"
                else:
                    cmd = cmd + curr

                hostinstance = hostinstance + 1

        if cmd not in fullcmd:
            fullcmd.append(cmd)

    # in max case we set the order back to ascending and
    # and just send the max load for execution 
    if original_order == MAX:
        fullcmd = [fullcmd[len(fullcmd)-1]]

    execute_run(fullcmd, len(hostsplits), curr_dir)

if __name__ == "__main__":
    main(sys.argv[1:])
