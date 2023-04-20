#!/bin/sh

# Workload: webserver-ro
# echo "Read only"
# for thread in $(seq 1 5); do
#     echo "Running Workload: webserver-ro threads = $thread"
#     for run in $(seq 1 5); do
#         echo "run = {$run}"
#         sleep 10
#         EXECUTABLE=/users/bbritto/workspace/uFS/cfs/build/test/client-retry/workload/webserver
#         $EXECUTABLE -p 1,3 -n $thread -f 2 -r 100 -i 1024 -t 0

#         # kill server
#         sudo killall fsMain
#     done
# done

# Workload: webserver-rw
echo "Read write"
for thread in $(seq 4 5); do
    echo "Running Workload: webserver-rw threads = $thread"
    for run in $(seq 1 5); do
        echo "run = {$run}"
        sleep 10
        EXECUTABLE=/users/bbritto/workspace/uFS/cfs/build/test/client-retry/workload/webserver
        $EXECUTABLE -p 1,3 -n $thread -f 2 -r 100 -w 100 -i 1024 -t 1

        # kill server
        sudo killall fsMain
    done
done


# Workload: varmail
# echo "varmail"
# for thread in 1 2 3 6; do
#     echo "Running Workload: varmail threads = $thread"
#     for run in $(seq 1 5); do
#         echo "------ run = $run ------"

#         sleep 15

#         # init
#         echo "------ calling init ------"
#         EXECUTABLE=/users/bbritto/workspace/uFS/cfs/build/test/client-retry/workload/initWorkload
#         $EXECUTABLE -p 1,3 -d 6 -f 3 -s 1 -t 1

#         # kill server gracefully
#         ps -ef | grep fsMain | grep -v grep | awk '{print $2}' | sudo xargs kill -SIGINT

#         sleep 15

#         # workload
#         echo "------ calling workload ------"
#         EXECUTABLE=/users/bbritto/workspace/uFS/cfs/build/test/client-retry/workload/varmail
#         $EXECUTABLE -p 1,3 -n $thread -r 1 -w 1024   -i 1024

#         # kill server
#         sudo killall fsMain
#     done
# done