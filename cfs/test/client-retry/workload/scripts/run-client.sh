#!/bin/sh

# Workload: webserver-ro
for thread in $(seq 1 6); do
    echo "Running Workload: webserver-ro threads = $thread"
    for run in $(seq 1 5); do
        echo "run = {$run}"
        sleep 10
        EXECUTABLE=/users/bbritto/workspace/uFS/cfs/build/test/client-retry/workload/webserver-ro
        $EXECUTABLE -p 1,3 -n $thread -f 10 -s 2 -i 1024

        # kill server
        sudo killall fsMain
    done
done
