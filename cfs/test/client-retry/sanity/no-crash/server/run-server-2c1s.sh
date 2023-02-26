#!/bin/sh
BLUE='\033[0;44m'
NOCOLOR='\033[0m'

EXECUTABLE=/users/bbritto/workspace/uFS/cfs/build/fsMain
EXIT_PATH=/tmp/cfs_exit
SPDK_CONF=/users/bbritto/workspace/uFS/cfs/build/spdk_dev.conf
FSP_CONF=/users/bbritto/workspace/uFS/cfs/build/fsp.conf
NUM_WORKERS=1
WORKER_CORE_LIST=1
NUM_APP_PROC=2
SHM_BASE_OFFSET_LIST=5

echo "${BLUE} Invoking Server ${NOCOLOR}"
$EXECUTABLE $NUM_WORKERS $NUM_APP_PROC $SHM_BASE_OFFSET_LIST $EXIT_PATH $SPDK_CONF $WORKER_CORE_LIST $FSP_CONF
echo "${BLUE} Exiting Server ${NOCOLOR}"
