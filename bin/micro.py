#!/usr/bin/env python
__author__ = 'Constantino Gomes'


import os
import sys
import subprocess

algorithm = [
    "SI",
    "NMSI"
]

time = 30000

#threads = [1,2,4,8,16,32,64]
# opsArr = [10,50,100,500,1000]
# perc_read_only = [50, 45, 40, 30, 20, 10]
# perc_write = [2,5,10,15]

threads = [1,2,4,8,16,32,64]
opsArr = [50]
perc_read = [20]
perc_read_only = [2]

for thread in threads:
    for x in range(0,len(perc_read_only)):
        for y in range(0,len(perc_write)):
            for ops in opsArr:
                for alg in  algorithm:
                    command = ["java", "-Xmx20g", "-Xms8g", "-cp", "target/myhashdb-1.0.3.jar", \
                                    "bench.MicroSI", alg ,str(time), str(thread),str(perc_read_only[x]) ,str(ops), str(perc_write[y]) ]
                    print " ".join(command)
                    # subprocess.call(command)
