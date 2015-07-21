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
clientsPerWare = [1]
warehouses = range(1,21) # 1 a 20


for nware in warehouses:
    for nclients in clientsPerWare:
        for alg in  algorithm:
            ycsb_command = ["java", "-Xmx30g", "-Xms5g", "-cp", "target/myhashdb-1.0.3-jar-with-dependencies.jar:", \
                            "bench.tpcc.TpccEmbeded", "-w", str(nware), "-c", str(nclients), "-t", str(time), "-tp", alg, "-B" ]
            subprocess.call(ycsb_command)
