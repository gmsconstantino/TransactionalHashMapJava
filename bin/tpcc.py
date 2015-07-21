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
clientsPerWare = [1,2,3,4,5]
warehouses = [1,2,4,8]


for nware in warehouses:
    for nclients in clientsPerWare:
        for alg in  algorithm:
            ycsb_command = ["java", "-Xmx20g", "-Xms5g", "-cp", "target/myhashdb-1.0.3-jar-with-dependencies.jar:", \
                            "bench.tpcc.TpccEmbeded", "-w", str(nware), "-c", str(nclients), "-t", str(time), "-tp", alg, "-B" ]
            subprocess.call(ycsb_command)
