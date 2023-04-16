#!/bin/sh

# run workload init
EXECUTABLE=/users/bbritto/workspace/uFS/cfs/build/test/client-retry/workload/initWorkload
$EXECUTABLE -p 1,3 -d 6 -f 10 -s 2

# kill server
ps -ef | grep fsMain | grep -v grep | awk '{print $2}' | sudo xargs kill -SIGINT