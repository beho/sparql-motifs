#!/bin/bash

DATASET="swdf"

. environment.sh
JAVA_OPTS="-Xms2048M -Xmx4096M -XX:+UseConcMarkSweepGC"

scala -cp target/sparql-motifs-0.0.1.jar $MOTIF_CMD_CLASS -role enumerator -dataset $DATASET -skip-pvq $@
