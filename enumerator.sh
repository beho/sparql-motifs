#!/bin/bash

. environment.sh

# PORT=52222
DATASET="swdf"

# COUNTER_HOST="10.0.0.3"
# COUNTER_PORT=52223

# echo ""
# echo "connecting to : ${COUNTER_HOST}:${COUNTER_PORT}"
# echo ""

scala -cp target/sparql-motifs-0.0.1.jar $CMD_CLASS -role enumerator -dataset $DATASET -skip-pvq $@
