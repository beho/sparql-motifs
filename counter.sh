#!/bin/sh

. environment.sh

PORT=52223
DATASET="swdf-12"

$SCALA_HOME/bin/scala scripts/motifs-run.scala -role counter $PORT $DATASET $ENUMERATORS_COUNT