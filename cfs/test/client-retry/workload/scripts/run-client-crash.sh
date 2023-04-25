#!/bin/sh

for run in $(seq 1 4); do
    sleep 5
    EXECUTABLE=/users/bbritto/workspace/uFS/cfs/build/test/client-retry/workload/webserver
    $EXECUTABLE -p 1,4,3 -n 2 -f 10 -i 4096 -t 7

    sudo killall fsMain
done
