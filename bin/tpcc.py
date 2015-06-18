#!/usr/bin/env python
__author__ = 'Constantino Gomes'


import os
import sys
import subprocess

algorithm = [
    "SI",
    "NMSI"
]

time = 60
clients = [1,2,3,4,5,6,7,8,9,10]
threads = [1,2,4,8]


for thread in threads:
    for c in clients:
        for alg in  algorithm:
            ycsb_command = ["java", "-Xmx20g", "-Xms15g", "-cp", "target/myhashdb-1.0.3.jar:/local/cj.gomes/thrift-0.9.2/lib/java/build/libthrift-0.9.2.jar",  \
                            "bench.tpcc.TpccEmbeded", "-w", str(thread), "-c", str(thread*c), "-t", str(time), "-tp", alg, "-B" ]
            subprocess.call(ycsb_command)
