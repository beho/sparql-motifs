#!/bin/sh

. environment.sh

PORT=52223
# DATASET="swdf-12"

# $SCALA_HOME/bin/scala scripts/motifs-run.scala -role counter $PORT $DATASET $ENUMERATORS_COUNT
scala -cp target/sparql-motifs-0.0.1.jar $CMD_CLASS -role counter $PORT