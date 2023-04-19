#!/bin/sh

# Run server webserver
# for repeat in $(seq 1 6); do
#     for run in $(seq 1 5); do
#         EXECUTABLE=/users/bbritto/workspace/uFS/cfs/build/fsMain
#         EXIT_PATH=/tmp/cfs_exit
#         SPDK_CONF=/users/bbritto/workspace/uFS/cfs/build/spdk_dev.conf
#         FSP_CONF=/users/bbritto/workspace/uFS/cfs/build/fsp.conf
#         NUM_WORKERS=1
#         WORKER_CORE_LIST=1
#         NUM_APP_PROC=1
#         SHM_BASE_OFFSET_LIST=1
#         CRASH_NUM=-1
#         sudo $EXECUTABLE $NUM_WORKERS $NUM_APP_PROC $SHM_BASE_OFFSET_LIST $EXIT_PATH $SPDK_CONF $WORKER_CORE_LIST $FSP_CONF $CRASH_NUM
#     done
# done

# Run server varmail
for repeat in 1 2 3 6; do
    for run in $(seq 1 5); do
        # mkfs
        EXECUTABLE=/users/bbritto/workspace/uFS/cfs/build/test/fsproc/testRWFsUtil
        sudo $EXECUTABLE mkfs

        # server
        EXECUTABLE=/users/bbritto/workspace/uFS/cfs/build/fsMain
        EXIT_PATH=/tmp/cfs_exit
        SPDK_CONF=/users/bbritto/workspace/uFS/cfs/build/spdk_dev.conf
        FSP_CONF=/users/bbritto/workspace/uFS/cfs/build/fsp.conf
        NUM_WORKERS=1
        WORKER_CORE_LIST=1
        NUM_APP_PROC=1
        SHM_BASE_OFFSET_LIST=1
        CRASH_NUM=-1
        sudo $EXECUTABLE $NUM_WORKERS $NUM_APP_PROC $SHM_BASE_OFFSET_LIST $EXIT_PATH $SPDK_CONF $WORKER_CORE_LIST $FSP_CONF $CRASH_NUM

        # Run checkpoint
        EXECUTABLE=/users/bbritto/workspace/uFS/cfs/build/test/fsproc/fsProcOfflineCheckpointer
        sudo $EXECUTABLE

        # server
        EXECUTABLE=/users/bbritto/workspace/uFS/cfs/build/fsMain
        sudo $EXECUTABLE $NUM_WORKERS $NUM_APP_PROC $SHM_BASE_OFFSET_LIST $EXIT_PATH $SPDK_CONF $WORKER_CORE_LIST $FSP_CONF $CRASH_NUM
    done
done