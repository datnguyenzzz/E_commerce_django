#!/bin/sh

echo "Waiting nimbus come online ...."
/topology-deployment/wait-for.sh nimbus:6627 --timeout=3600000 -- echo "Nimbus is online ..."
echo "Waiting kafka come online ...."
/topology-deployment/wait-for.sh kafka:9092 --timeout=3600000 -- echo "Kafka is online ..."
echo "Waiting redis come online ...."
/topology-deployment/wait-for.sh redis:6379 --timeout=3600000 -- echo "Redis is online ..."
echo "Waiting cassandra come online ...."
/topology-deployment/wait-for.sh cassandra2:9042 --timeout=3600000 -- echo "Cassandra is online ..."
/apache-storm-*/bin/storm jar /topology-definition/recommender-real-time-service-1.0-SNAPSHOT.jar vn.datnguyen.recommender.TopologyDefinition Recommender-Realtime-Topology

sleep infinity