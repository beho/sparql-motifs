#!/bin/bash

. environment.sh
# JVM_ARGS="-Xms2048M -Xmx4096M -XX:+UseConcMarkSweepGC"

# PORT=52222
DATASET="swdf"

# echo ""
# echo "connecting to : ${COUNTER_HOST}:${COUNTER_PORT}"
# echo ""

scala -cp target/sparql-motifs-0.0.1.jar $MOTIF_CMD_CLASS -role enumerator -dataset $DATASET -skip-pvq $@
