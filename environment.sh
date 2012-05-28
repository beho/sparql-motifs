#!/bin/sh

JAR_PATH=target/sparql-motifs-0.0.1.jar
CMD_CLASS=motifs.commandline.Executor


BASE_PATH=`pwd`


# LIB_PATH=$BASE_PATH/lib

# LIB_PATH=../Query/lib

SCRIPTS_PATH=scala-scripts

# export JAVA_HOME=/Library/Java/Home
# JAVA_OPTS="-Xmx2048M -XX:+UseConcMarkSweepGC"
JVM_ARGS="-Xmx2048M -XX:+UseConcMarkSweepGC"

. classpath.sh

# export CLASSPATH=$LIB_PATH/TDB-0.8.10/lib/*:$LIB_PATH/jgrapht-0.8.2/jgrapht-jdk1.6.jar:$LIB_PATH/akka-2.0.1/lib/scala-library.jar:$LIB_PATH/akka-2.0.1/lib/akka/*:$LIB_PATH/smack/*:.
# export CLASSPATH=target/sparql-motifs-0.0.1.jar:$CLASSPATH

# SCALA_HOME=$BASE_PATH/scala 
SCALA_HOME=/opt/local/share/scala-2.9

# AKKA_HOME=$LIB_PATH/akka-2.0.1
export TDBROOT=$LIB_PATH/TDB-0.8.10

PATH=$PATH:$SCALA_HOME/bin:$TDBROOT/bin

echo "current directory : ${BASE_PATH}"
echo ""

echo ""
# echo "lib               : ${LIB_PATH}"
echo "scala             : ${SCALA_HOME}"
echo "JVM args          : ${JVM_ARGS}"
# echo "classpath         : ${CLASSPATH}"
echo ""
echo ""
echo ""