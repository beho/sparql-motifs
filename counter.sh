#!/bin/sh

. environment.sh

scala -cp target/sparql-motifs-0.0.1.jar $MOTIF_CMD_CLASS -role counter -port $@