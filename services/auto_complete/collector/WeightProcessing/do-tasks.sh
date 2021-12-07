#!/bin/bash

TARGET_ID=$(date +%Y%m%d_%H%M)

echo "Checking if /words/kafka_sink/words-from-client/ exists.."
while ! $(hadoop fs -test -d "/words/kafka_sink/words-from-client/") ; do \
    echo "Waiting for folder /words/kafka_sink/words-from-client/ to be created by kafka connect. Please wait.."; 
done

echo "Checking for contents in /words/kafka_sink/words-from-client/.."
while [[ $(hadoop fs -ls /words/kafka_sink/words-from-client/ | sed 1,1d) == "" ]] ; do \
    echo "Waiting for kafka connect to populate/words/kafka_sink/words-from-client/*. Please wait.."; 
done

# CREATE WEIGHT HDFS BATCH
hadoop fs -mkdir -p /words/2_with_weight/${TARGET_ID}/

echo "STARTING ADD-WEIGHT SERVICE ...."
cd /HADOOP_TASKS/adding-words-weight
mvn package
java -cp target/adding-words-weight-1.0-SNAPSHOT.jar com.mycompany.app.App