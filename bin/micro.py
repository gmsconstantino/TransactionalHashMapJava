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

threads = [1,2,4,8,16,24,32,48,64]

opsArr = [10,50,100,500,1000]

perc_read = [50, 45, 40, 30, 20, 10]
perc_write = [2,5,10,15]

for thread in threads:
    for x in range(0,len(perc_read)):
        for ops in opsArr:
            for alg in  algorithm:
                ycsb_command = ["java", "-Xms8g", "-cp", "target/myhashdb-1.0.3.jar", \
                                "bench.MicroSI", alg ,str(time), str(thread),str(perc_read[x]) ,str(ops), str(perc_write[x]) ]
                subprocess.call(ycsb_command)
