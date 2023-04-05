#!/bin/sh

# # Workload: seqread
# echo "Running Workload: seqread"
# for run in $(seq 0 5); do
#     echo "run = {$run}"
#     sleep 10
#     EXECUTABLE=/users/bbritto/workspace/uFS/cfs/build/test/client-retry/microbenchmark/seqread
#     $EXECUTABLE 1,3

#     # kill server
#     sudo killall fsMain
# done

# Workload: random read
echo "Running Workload: random read"
for run in $(seq 0 5); do
    echo "run = {$run}"
    sleep 10
    EXECUTABLE=/users/bbritto/workspace/uFS/cfs/build/test/client-retry/microbenchmark/rread
    $EXECUTABLE 1,3

    # kill server
    sudo killall fsMain
done