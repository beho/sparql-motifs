#!/bin/bash

. environment.sh

./merge.sh $@
# scala -cp $JAR_PATH $MERGE_CMD_CLASS $@