#!/bin/sh
BLUE='\033[0;44m'
NOCOLOR='\033[0m'

EXECUTABLE=/users/bbritto/workspace/uFS/cfs/build/test/client/testAppCli
COMMAND_PATH=/users/bbritto/workspace/uFS/cfs/test/client-retry/commands
SHM_OFFSET_LIST=7

echo "${BLUE} Invoking Client with Test = Simple Read Write ${NOCOLOR}"
$EXECUTABLE $SHM_OFFSET_LIST < $COMMAND_PATH/simple-pread-pwrite
echo "${BLUE} Exiting Client with Test = Simple Read Write ${NOCOLOR}"

echo "${BLUE} Invoking Client with Test = Metadata Ops ${NOCOLOR}"
$EXECUTABLE $SHM_OFFSET_LIST < $COMMAND_PATH/metadata-ops
echo "${BLUE} Exiting Client with Test = Read Ops ${NOCOLOR}"

echo "${BLUE} Invoking Client with Test = CRUD ${NOCOLOR}"
$EXECUTABLE $SHM_OFFSET_LIST < $COMMAND_PATH/crud
echo "${BLUE} Exiting Client with Test = CRUD ${NOCOLOR}"