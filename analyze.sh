#!/bin/bash

. environment.sh

DATASET="dbpedia"

scala -cp target/sparql-motifs-0.0.1.jar $MOTIF_CMD_CLASS -role both -dataset $DATASET -skip-pvq $@