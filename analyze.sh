#!/bin/bash

. environment.sh

DATASET="dbpedia"

# nohup $SCALA_HOME/bin/scala scripts/motifs-run.scala -role both -dataset $DATASET $@ > counter-$DATASET.out &
scala $SCRIPTS_PATH/motifs-run.scala -role both -dataset $DATASET -skip-pvq $@