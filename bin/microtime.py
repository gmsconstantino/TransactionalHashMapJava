#!/usr/bin/env python
__author__ = 'Constantino Gomes'


import os
import sys
import subprocess

algorithm = [
    "SI",
    "NMSI"
]

time = 30

#threads = [1,2,4,8,16,32,64]
# opsArr = [10,50,100,500,1000]
# perc_read_only = [50, 45, 40, 30, 20, 10]
# perc_write = [2,5,10,15]

threads = [1,2,4,8,16,24,32,48,64]

for thread in threads:
    for alg in  algorithm:
        command = ["java", "-Xmx40g", "-Xms40g", "-cp", "target/myhashdb-1.0.3-jar-with-dependencies.jar", \
                        "bench.MicroTimeout", "-alg",alg, "-d", str(time), \
                        "-t", str(thread), "-s", "2", "-c", "0", "-r", "50"]
        # print " ".join(command)
        subprocess.call(command)
