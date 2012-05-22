#!/bin/bash

. environment.sh

PORT=52222
DATASET="swdf"

COUNTER_HOST="10.0.0.3"
COUNTER_PORT=52223

echo ""
echo "connecting to : ${COUNTER_HOST}:${COUNTER_PORT}"
echo ""

$SCALA_HOME/bin/scala scripts/motifs-run.scala -role enumerator $PORT $COUNTER_HOST $COUNTER_PORT -dataset $DATASET $@
