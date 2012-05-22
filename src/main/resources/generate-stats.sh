#!/bin/bash

echo $@
echo "generating statistics"
tdbstats --loc=$@ --graph=urn:x-arq:UnionGraph > stats.opt

echo "moving to dir"
mv stats.opt $@