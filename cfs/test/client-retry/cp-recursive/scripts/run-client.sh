#!/bin/sh

for crashRequestNum in $(seq 0 85); do
    echo "crashRequestNum = {$crashRequestNum}"
    sleep 10
    start=`date +%s%N`
    LD_PRELOAD=/users/bbritto/workspace/syscall_intercept/build/examples/libsyscall_fsp.so cp -r FSPsrc FSPdest
    end=`date +%s%N`
    echo Execution time was `expr $end - $start` nanoseconds when crash num was $crashRequestNum.
    ps -ef | grep fsMain | grep -v grep | awk '{print $2}' | sudo xargs kill
done

# crashRequestNum=0
# # Run server with crash
# until [ $crashRequestNum -lt 85 ]
# do
#     echo "crashRequestNum = {$crashRequestNum}"
# #    sleep 10
# #    LD_PRELOAD=/users/bbritto/workspace/syscall_intercept/build/examples/libsyscall_fsp.so cp -r FSPsrc FSPdest
# #    ps -ef | grep fsMain | grep -v grep | awk '{print $2}' | sudo xargs kill
# done