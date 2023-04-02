#!/bin/sh

# Run server with crash
for crashRequestNum in $(seq 0 85); do
    echo "crashRequestNum = {$crashRequestNum}"
    EXECUTABLE=/users/bbritto/workspace/uFS/cfs/build/fsMain
    EXIT_PATH=/tmp/cfs_exit
    SPDK_CONF=/users/bbritto/workspace/uFS/cfs/build/spdk_dev.conf
    FSP_CONF=/users/bbritto/workspace/uFS/cfs/build/fsp.conf
    NUM_WORKERS=1
    WORKER_CORE_LIST=1
    NUM_APP_PROC=1
    SHM_BASE_OFFSET_LIST=1
    CRASH_NUM=$crashRequestNum
    sudo $EXECUTABLE $NUM_WORKERS $NUM_APP_PROC $SHM_BASE_OFFSET_LIST $EXIT_PATH $SPDK_CONF $WORKER_CORE_LIST $FSP_CONF $CRASH_NUM

    # Run server without crash
    CRASH_NUM=-1
    sudo $EXECUTABLE $NUM_WORKERS $NUM_APP_PROC $SHM_BASE_OFFSET_LIST $EXIT_PATH $SPDK_CONF $WORKER_CORE_LIST $FSP_CONF $CRASH_NUM
done