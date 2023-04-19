#!/bin/sh

# run workload init webserver
# EXECUTABLE=/users/bbritto/workspace/uFS/cfs/build/test/client-retry/workload/initWorkload
# $EXECUTABLE -p 1,3 -d 6 -f 10 -s 1 -t 0

# # kill server
# ps -ef | grep fsMain | grep -v grep | awk '{print $2}' | sudo xargs kill -SIGINT

# run workload init varmail
EXECUTABLE=/users/bbritto/workspace/uFS/cfs/build/test/client-retry/workload/initWorkload
$EXECUTABLE -p 1,3 -d 6 -f 3 -s 1 -t 1

# kill server
ps -ef | grep fsMain | grep -v grep | awk '{print $2}' | sudo xargs kill -SIGINT