#!/bin/sh

curl -s \
     -X "POST" "http://localhost:8083/connectors" \
     -H "Content-Type: application/json" \
    --data '{ 
        "name": "hdfs-sink-phrases", 
        "config": { 
            "connector.class": "io.confluent.connect.hdfs3.Hdfs3SinkConnector", 
            "partitioner.class": "io.confluent.connect.storage.partitioner.TimeBasedPartitioner",
            "partition.duration.ms": '$PARTITION_DURATION',
            "path.format": "YYYYMMdd_HHmm",
            "timestamp.extractor":"Record",
            "confluent.topic.bootstrap.servers":"'$CONNECT_BOOTSTRAP_SERVERS'",
            "tasks.max": "'$TASKS_MAX'", 
            "topics": "'$TOPIC_NAME'", 
            "hdfs.url": "hdfs://hdfs-namenode:9000", 
            "flush.size": "'$FLUSH_SIZE'", 
            "locale": "en",
            "timezone": "UTC",
            "topics.dir": "'$TOPIC_DIR'",
            "logs.dir": "'$LOG_DIR'",
            "name": "hdfs-sink-phrases" 
        } 
    }'