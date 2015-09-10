#!/usr/bin/env python
__author__ = 'Constantino Gomes'


import os
import sys
import subprocess
import time

def usage():
    print "Usage: %s time(s) size_partition read_perc share [debug]" % sys.argv[0]
    sys.exit(1)



threads = [1,2,4,8,16,32,64]
partitions = [1,2,4,8,16,32,64]

if len(sys.argv) < 5:
    usage()

exectime = sys.argv[1]
size_partition = sys.argv[2]
read_perc = sys.argv[3]
share = sys.argv[4]
debug = 0

if len(sys.argv) > 5:
    debug = sys.argv[5]


for thread in threads:
    for partition in partitions:
        if thread < partition:
            continue

        command = ["java", "-Xmx40g", "-Xms40g", "-cp", "target/myhashdb-1.0.3.jar", \
                        "bench.MicroStorage", "-d" ,str(exectime), "-t", str(thread), \
                   "-p", str(partition), "-sz", str(size_partition), "-r", str(read_perc),\
                   "-sh", str(share),"-debug", str(debug)]
        # print " ".join(command)
        subprocess.call(command)
print "\n"
