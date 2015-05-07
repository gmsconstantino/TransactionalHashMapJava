#!/usr/bin/env python
__author__ = 'Constantino Gomes'


import os
import sys
import subprocess

BASE_URL = "https://github.com/gmsconstantino/TransactionalHashMapJava"
COMMANDS = {
    "run" : {
        "command"     : "",
        "description" : "Execute the Micro Benchmark",
        "main"        : "bench.Micro",
        }
    }

DATABASES = {
    "myhashmap"    : "fct.thesis.bindings.MicroBinding",
    }

OPTIONS = {
    "-P file"      : "Specify workload file",
    "-p key=value" : "Override workload property",
    "-s"           : "Print status to stderr",
    "-target n"    : "Target ops/sec (default: unthrottled)",
    "-threads n"   : "Number of client threads (default: 1)",
    }

def usage():
    print "Usage: %s command database [options]" % sys.argv[0]

    print "\nCommands:"
    for command in sorted(COMMANDS.keys()):
        print "    %s %s" % (command.ljust(13), COMMANDS[command]["description"])

    print "\nDatabases:"
    for db in sorted(DATABASES.keys()):
        print "    %s %s" % (db.ljust(13), BASE_URL + db.split("-")[0])

    print "\nOptions:"
    for option in sorted(OPTIONS.keys()):
        print "    %s %s" % (option.ljust(13), OPTIONS[option])

    print """\nWorkload Files:
    There are various predefined workloads under workloads/ directory.
    See https://github.com/brianfrankcooper/YCSB/wiki/Core-Properties
    for the list of workload properties."""

    sys.exit(1)

def find_jars(dir, database):
    jars = []
    for (dirpath, dirnames, filenames) in os.walk(dir):
        if dirpath.endswith("conf"):
            jars.append(dirpath)
        for filename in filenames:
            if filename.endswith(".jar") and \
                    (filename.startswith("core") or \
                             filename.startswith(database.split("-")[0]) or \
                             not "binding" in filename):
                jars.append(os.path.join(dirpath, filename))
    return jars

def get_micro_home():
    dir = os.path.abspath(os.path.dirname(sys.argv[0]))
    while "CHANGELOG" not in os.listdir(dir):
        dir = os.path.join(dir, os.path.pardir)
    return os.path.abspath(dir)

if len(sys.argv) < 3:
    usage()
if sys.argv[1] not in COMMANDS:
    print "ERROR: Command '%s' not found" % sys.argv[1]
    usage()
if sys.argv[2] not in DATABASES:
    print "ERROR: Database '%s' not found" % sys.argv[2]
    usage()

micro_home = get_micro_home()
command = COMMANDS[sys.argv[1]]["command"]
database = sys.argv[2]
db_classname = DATABASES[database]
options = sys.argv[3:]

print os.pathsep.join(find_jars(micro_home, database))

ycsb_command = ["java", "-Xmx20g", "-Xms2g", "-cp", os.pathsep.join(find_jars(micro_home, database)), \
                COMMANDS[sys.argv[1]]["main"], "-db", db_classname] + options
if command:
    ycsb_command.append(command)
#print " ".join(ycsb_command)
subprocess.call(ycsb_command)
