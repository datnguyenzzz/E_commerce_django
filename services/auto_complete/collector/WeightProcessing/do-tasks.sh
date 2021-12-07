#!/bin/bash

hadoop fs -ls /words

echo "STARTING ADD-WEIGHT SERVICE ...."
cd /HADOOP_TASKS/adding-words-weight
mvn package
java -cp target/adding-words-weight-1.0-SNAPSHOT.jar com.mycompany.app.App