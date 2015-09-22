#!/usr/bin/env python
__author__ = 'Constantino Gomes'


import os
import sys
import subprocess
import time

def usage():
    print "Usage: %s clientsPerWare warehouses" % sys.argv[0]
    sys.exit(1)

if len(sys.argv) < 3:
    usage()

algorithm = [
    "SI",
    "NMSI"
]

test_time = 30
clientsPerWare = sys.argv[1]
warehouses = sys.argv[2]

for i in range(1,4):
    print "### Execution "+str(i)
    sys.stdout.flush()

    for alg in  algorithm:
        ycsb_command = ["java", "-Xmx40g", "-Xms40g", "-cp", "target/myhashdb-1.0.3-jar-with-dependencies.jar:", \
                        "bench.tpcc.TpccEmbeded", "-w", warehouses, "-c", clientsPerWare, "-t", str(test_time), "-tp", alg, "-B" ]
        subprocess.call(ycsb_command)
        time.sleep(5)

    print "\n"

subprocess.Popen('cat /home/a41903/mail.txt | /local/cj.gomes/mail/msmtp/bin/msmtp -t cj.gomes@campus.fct.unl.pt', shell=True)
subprocess.Popen('cp /local/cj.gomes/result/* ~/raw', shell=True)