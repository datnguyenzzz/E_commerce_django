#!/bin/bash

TASKNAME=$1
echo $TASKNAME
cd /HADOOP_TASKS/${TASKNAME}
mkdir classes
CLASSPATH=".:../avro/*:$(hadoop classpath)" 
javac -cp ${CLASSPATH} -d ./classes ${TASKNAME}.java
jar cf ${TASKNAME}.jar -C classes .