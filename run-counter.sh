#!/bin/bash

. environment.sh

scala -cp $JAR_PATH $MOTIF_CMD_CLASS -role counter -port $@