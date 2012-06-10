#!/bin/bash

. environment.sh

scala -cp $JAR_PATH $VIZ_CMD_CLASS $@