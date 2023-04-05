#!/bin/sh

# Workload: seqread
for run in $(seq 0 5); do
    echo "run = {$run}"
    sleep 10
    EXECUTABLE=/users/bbritto/workspace/uFS/cfs/build/test/client-retry/microbenchmark/seqread
    $EXECUTABLE 1,3

    # kill server
    sudo killall fsMain
done