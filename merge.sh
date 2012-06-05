#!/bin/sh

. environment.sh

scala -cp target/sparql-motifs-0.0.1.jar $MERGE_CMD_CLASS $@