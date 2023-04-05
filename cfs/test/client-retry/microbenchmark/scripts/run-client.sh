#!/bin/sh

# Workload: seqread
EXECUTABLE=/users/bbritto/workspace/uFS/cfs/build/test/client-retry/microbenchmark/seqread
$EXECUTABLE 1,3

# kill server
sudo killall fsMain