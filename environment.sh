#!/bin/sh

JAR_PATH=target/sparql-motifs-0.0.1.jar
MOTIF_CMD_CLASS=motifs.commandline.MotifExecutor
MERGE_CMD_CLASS=motifs.commandline.MergeExecutor

# JAVA_OPTS is pass by scala to java
JAVA_OPTS="-Xmx2048M -XX:+UseConcMarkSweepGC"

. classpath.sh

# export CLASSPATH=$LIB_PATH/TDB-0.8.10/lib/*:$LIB_PATH/jgrapht-0.8.2/jgrapht-jdk1.6.jar:$LIB_PATH/akka-2.0.1/lib/scala-library.jar:$LIB_PATH/akka-2.0.1/lib/akka/*:$LIB_PATH/smack/*:.
# export CLASSPATH=target/sparql-motifs-0.0.1.jar:$CLASSPATH

export SCALA_HOME=/opt/local/share/scala-2.9
export TDBROOT=/Users/beho/Projects/Query/lib/TDB-0.8.10

PATH=$PATH:$SCALA_HOME/bin:$TDBROOT/bin

echo "scala             : ${SCALA_HOME}"
echo "tdbroot           : ${TDBROOT}"
echo "JVM args          : ${JVM_ARGS}"
echo "classpath         : ${CLASSPATH}"
echo ""
echo ""
