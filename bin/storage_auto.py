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

exectime=60
for share in [0, 1]:
     for read_perc in [100, 95]:
         for size_partition in [1000, 1000000]:
            for i in range(1,4):
                print "### Execution %s - Time %s(s), Size Partition %s, Read_Perc %s%%, Share %s" % (str(i),exectime,size_partition,read_perc,share)
                sys.stdout.flush()

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
                        time.sleep(5)

                print "\n"

            print "\n\n"