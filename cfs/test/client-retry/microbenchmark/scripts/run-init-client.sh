#!/bin/sh

# run workload init
EXECUTABLE=/users/bbritto/workspace/uFS/cfs/build/test/client-retry/microbenchmark/init
$EXECUTABLE 1,3 0

# kill server
sudo killall fsMain