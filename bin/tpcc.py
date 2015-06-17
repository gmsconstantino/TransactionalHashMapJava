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

threads = [1,2,4,8,16,24,32,48,64]


for thread in threads:
    for alg in  algorithm:
        ycsb_command = ["java", "-Xmx20g", "-Xms15g", "-cp", "target/myhashdb-1.0.3.jar:/local/cj.gomes/thrift-0.9.2/lib/java/build/libthrift-0.9.2.jar",  \
                        "bench.tpcc.TpccEmbeded", "-w", str(thread), "-c", str(thread), "-t", str(time), "-tp", alg, "-B" ]
        subprocess.call(ycsb_command)
