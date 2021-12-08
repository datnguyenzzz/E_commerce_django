#!/bin/bash

TARGET_ID=$(date +%Y%m%d_%H%M)
MAX_NUMBER_OF_INPUT_FOLDERS="3"

echo "Checking if /words/kafka_sink/words-from-client/ exists.."
while ! $(hadoop fs -test -d "/words/kafka_sink/words-from-client/") ; do \
    echo "Waiting /words/kafka_sink/words-from-client/ to be created by kafka sink ..."; 
done

echo "Checking for contents in /words/kafka_sink/words-from-client/.."
while [[ $(hadoop fs -ls /words/kafka_sink/words-from-client/ | sed 1,1d) == "" ]] ; do \
    echo "Waiting for kafka sink to populate /words/kafka_sink/words-from-client/* ..."; 
done

# CREATE WEIGHT HDFS BATCH
hadoop fs -mkdir -p /words/2_with_weight/${TARGET_ID}/

echo "STARTING ADD-WEIGHT SERVICE ...."
INPUT_BATCH=`hadoop fs -ls /words/kafka_sink/words-from-client/ | sed 1,1d | sort -r -k8 | awk '{print \$8}' | head -${MAX_NUMBER_OF_INPUT_FOLDERS} | sort  `
TASK_NAME="adding-words-weight"
JAR_FILEPATH="target/${TASK_NAME}-1.0-SNAPSHOT.jar"
APP_NAME="com.mycompany.app.App"

echo "${INPUT_BATCH}"

cd /HADOOP_TASKS/${TASK_NAME}
mvn package
java -cp ${JAR_FILEPATH} ${APP_NAME}

for input_folder in ${INPUT_BATCH}; do
    echo "Processing input: " $input_folder;
done;