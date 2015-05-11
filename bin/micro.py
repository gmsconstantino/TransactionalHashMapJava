#!/usr/bin/env python
__author__ = 'Constantino Gomes'


import os
import sys
import subprocess

algorithm = [
    "SI",
    "BLOTTER"
]

time = 30000

threads = [1,2,4,8,16,24,32,48,64]

opsArr = [2,4,8,16,24,32]

perc_read = [95,80,70,60 ,50, 40,5]
perc_write = [5,20,30,40 ,50, 60,95]

for thread in threads:
    for x in range(0,len(perc_read)):
        for ops in opsArr:
            for alg in  algorithm:
                ycsb_command = ["java", "-Xms8g", "-cp", "target/myhashdb-1.0.3.jar", \
                                "bench.MicroSI", alg ,str(time), str(thread),str(perc_read[x]) ,str(ops), str(perc_write[x]) ]
                subprocess.call(ycsb_command)
