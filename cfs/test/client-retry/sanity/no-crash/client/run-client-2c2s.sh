#!/bin/sh
BLUE='\033[0;44m'
NOCOLOR='\033[0m'

# User input
echo "${BLUE} Enter 2 comma separated SHM Offsets${NOCOLOR}"
read shm_offset_list
echo "${BLUE} SHM Offset is $shm_offset ${NOCOLOR}"

EXECUTABLE=/users/bbritto/workspace/uFS/cfs/build/test/client/testAppCli
COMMAND_PATH=/users/bbritto/workspace/uFS/cfs/test/client-retry/commands

echo "${BLUE} Invoking Client with Test = Simple Read Write ${NOCOLOR}"
$EXECUTABLE $shm_offset_list < $COMMAND_PATH/simple-pread-pwrite
echo "${BLUE} Exiting Client with Test = Simple Read Write ${NOCOLOR}"

echo "${BLUE} Invoking Client with Test = Metadata Ops ${NOCOLOR}"
$EXECUTABLE $shm_offset_list < $COMMAND_PATH/metadata-ops
echo "${BLUE} Exiting Client with Test = Read Ops ${NOCOLOR}"

echo "${BLUE} Invoking Client with Test = CRUD ${NOCOLOR}"
$EXECUTABLE $shm_offset_list < $COMMAND_PATH/crud
echo "${BLUE} Exiting Client with Test = CRUD ${NOCOLOR}"

echo "${BLUE} Invoking Client with Test = Inode reassign ${NOCOLOR}"
$EXECUTABLE $shm_offset_list < $COMMAND_PATH/multi-server-reassign
echo "${BLUE} Exiting Client with Test = Inode reassign ${NOCOLOR}"