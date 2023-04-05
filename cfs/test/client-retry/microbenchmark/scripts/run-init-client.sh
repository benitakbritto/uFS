#!/bin/sh

# run workload init
EXECUTABLE=/users/bbritto/workspace/uFS/cfs/build/test/client-retry/microbenchmark/init
$EXECUTABLE 1,3 0

# kill server
# ps -ef | grep fsMain | grep -v grep | awk '{print $2}' | sudo xargs kill -SIGINT