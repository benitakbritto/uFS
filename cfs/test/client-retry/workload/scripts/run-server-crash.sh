#!/bin/sh
 
## COMMON BEG
EXECUTABLE=/users/bbritto/workspace/uFS/cfs/build/fsMain
EXIT_PATH=/tmp/cfs_exit
SPDK_CONF=/users/bbritto/workspace/uFS/cfs/build/spdk_dev.conf
FSP_CONF=/users/bbritto/workspace/uFS/cfs/build/fsp.conf
NUM_WORKERS=2
WORKER_CORE_LIST=1,2
NUM_APP_PROC=1
SHM_BASE_OFFSET_LIST=1,4
## COMMON END

# -- NO CRASH --
CRASH_NUM=-1
sudo $EXECUTABLE $NUM_WORKERS $NUM_APP_PROC $SHM_BASE_OFFSET_LIST $EXIT_PATH $SPDK_CONF $WORKER_CORE_LIST $FSP_CONF $CRASH_NUM

# -- CRASH BEG --
CRASH_NUM=1000000
sudo $EXECUTABLE $NUM_WORKERS $NUM_APP_PROC $SHM_BASE_OFFSET_LIST $EXIT_PATH $SPDK_CONF $WORKER_CORE_LIST $FSP_CONF $CRASH_NUM

CRASH_NUM=-1
sudo $EXECUTABLE $NUM_WORKERS $NUM_APP_PROC $SHM_BASE_OFFSET_LIST $EXIT_PATH $SPDK_CONF $WORKER_CORE_LIST $FSP_CONF $CRASH_NUM

# -- CRASH MIDDLE --
CRASH_NUM=1000040
sudo $EXECUTABLE $NUM_WORKERS $NUM_APP_PROC $SHM_BASE_OFFSET_LIST $EXIT_PATH $SPDK_CONF $WORKER_CORE_LIST $FSP_CONF $CRASH_NUM

CRASH_NUM=-1
sudo $EXECUTABLE $NUM_WORKERS $NUM_APP_PROC $SHM_BASE_OFFSET_LIST $EXIT_PATH $SPDK_CONF $WORKER_CORE_LIST $FSP_CONF $CRASH_NUM

# -- CRASH END --
CRASH_NUM=1000089
sudo $EXECUTABLE $NUM_WORKERS $NUM_APP_PROC $SHM_BASE_OFFSET_LIST $EXIT_PATH $SPDK_CONF $WORKER_CORE_LIST $FSP_CONF $CRASH_NUM

CRASH_NUM=-1
sudo $EXECUTABLE $NUM_WORKERS $NUM_APP_PROC $SHM_BASE_OFFSET_LIST $EXIT_PATH $SPDK_CONF $WORKER_CORE_LIST $FSP_CONF $CRASH_NUM