#!/bin/sh

# run workload init
EXECUTABLE=/users/bbritto/workspace/uFS/cfs/build/test/client-retry/cp-recursive/initClientRetryCpWorkload
$EXECUTABLE 1,3

ps -ef | grep fsMain | grep -v grep | awk '{print $2}' | sudo xargs kill -SIGINT