#!/bin/sh

# Workload: webserver-ro
# echo "Read only"
# for thread in $(seq 1 6); do
#     echo "Running Workload: webserver-ro threads = $thread"
#     for run in $(seq 1 5); do
#         echo "run = {$run}"
#         sleep 10
#         EXECUTABLE=/users/bbritto/workspace/uFS/cfs/build/test/client-retry/workload/webserver
#         $EXECUTABLE -p 1,3 -n $thread -f 10 -s 2 -i 1024 -t 0

#         # kill server
#         sudo killall fsMain
#     done
# done

# Workload: webserver-rw
# echo "Read write"
# for thread in $(seq 1 6); do
#     echo "Running Workload: webserver-rw threads = $thread"
#     for run in $(seq 1 5); do
#         echo "run = {$run}"
#         sleep 10
#         EXECUTABLE=/users/bbritto/workspace/uFS/cfs/build/test/client-retry/workload/webserver
#         $EXECUTABLE -p 1,3 -n $thread -f 10 -s 1 -i 1024 -t 1

#         # kill server
#         sudo killall fsMain
#     done
# done


# TODO: Rerun init since delete is sync, remove from pending
# Workload: varmail
echo "varmail"
for thread in 1 2 3 6; do
    echo "Running Workload: varmail threads = $thread"
    for run in $(seq 1 5); do
        echo "------ run = $run ------"

        sleep 15

        echo "------ calling init ------"
        # init
        EXECUTABLE=/users/bbritto/workspace/uFS/cfs/build/test/client-retry/workload/initWorkload
        $EXECUTABLE -p 1,3 -d 6 -f 3 -s 1 -t 1

        # kill server gracefully
        ps -ef | grep fsMain | grep -v grep | awk '{print $2}' | sudo xargs kill -SIGINT

        sleep 15

        echo "------ calling workload ------"
        # workload
        EXECUTABLE=/users/bbritto/workspace/uFS/cfs/build/test/client-retry/workload/varmail
        $EXECUTABLE -p 1,3 -n $thread -r 1 -w 16 -i 1024

        # kill server
        sudo killall fsMain
    done
done