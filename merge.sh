#!/bin/sh

. environment.sh

# PORT=25552
# DATASET="swdf-12"

# $SCALA_HOME/bin/scala scripts/motifs-run.scala -role counter $PORT $DATASET $ENUMERATORS_COUNT
scala -cp target/sparql-motifs-0.0.1.jar scala-scripts/tdb-merge.scala $@