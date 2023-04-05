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
# echo "Running Workload: random read"
# for run in $(seq 0 5); do
#     echo "run = {$run}"
#     sleep 10
#     EXECUTABLE=/users/bbritto/workspace/uFS/cfs/build/test/client-retry/microbenchmark/rread
#     $EXECUTABLE 1,3

#     # kill server
#     sudo killall fsMain
# done

# Workload: seq write
# echo "Running Workload: seq write"
# for run in $(seq 1 5); do
#     echo "run = {$run}"
#     sleep 10
#     EXECUTABLE=/users/bbritto/workspace/uFS/cfs/build/test/client-retry/microbenchmark/seqwrite
#     $EXECUTABLE 1,3

#     # kill server
#     sudo killall fsMain
# done

# # Workload: random write
# echo "Running Workload: random write"
# for run in $(seq 1 5); do
#     echo "run = {$run}"
#     sleep 10
#     EXECUTABLE=/users/bbritto/workspace/uFS/cfs/build/test/client-retry/microbenchmark/rwrite
#     $EXECUTABLE 1,3

#     # kill server
#     sudo killall fsMain
# done

# Workload: random write sync
echo "Running Workload: random write"
for run in $(seq 1 5); do
    echo "run = {$run}"
    sleep 10
    EXECUTABLE=/users/bbritto/workspace/uFS/cfs/build/test/client-retry/microbenchmark/rwritesync
    $EXECUTABLE 1,3

    # kill server
    sudo killall fsMain
done