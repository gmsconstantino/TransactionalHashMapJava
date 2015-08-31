#!/usr/bin/env python
__author__ = 'Constantino Gomes'


import os
import sys
import subprocess
import time

algorithm = [
    "SI",
    "NMSI"
]

test_time = 60
clientsPerWare = [1]
warehouses = range(1,20+1) # Add +1 on range to include the number you want

for i in range(1,4):
    print "### Execution "+str(i)
    sys.stdout.flush()

    for nware in warehouses:
        for nclients in clientsPerWare:
            for alg in  algorithm:
                ycsb_command = ["java", "-Xmx40g", "-Xms40g", "-cp", "target/myhashdb-1.0.3-jar-with-dependencies.jar:", \
                                "bench.tpcc.TpccEmbeded", "-w", str(nware), "-c", str(nclients), "-t", str(test_time), "-tp", alg, "-B" ]
                subprocess.call(ycsb_command)
                time.sleep(5)

    print "\n"

subprocess.Popen('cat /home/a41903/mail.txt | /local/cj.gomes/mail/msmtp/bin/msmtp -t cj.gomes@campus.fct.unl.pt', shell=True)
subprocess.Popen('cp /local/cj.gomes/result/* ~/raw', shell=True)