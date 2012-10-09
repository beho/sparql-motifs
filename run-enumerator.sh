#!/bin/bash

DATASET="swdf"

. environment.sh
JAVA_OPTS="-Xms2048M -Xmx4096M -XX:+UseConcMarkSweepGC"

scala -cp $JAR_PATH $MOTIF_CMD_CLASS -role enumerator -dataset $DATASET $@
