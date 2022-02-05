#!/bin/sh

echo "Waiting nimbus come online ...."
/topology-deployment/wait-for-nimbus.sh nimbus:6627 --timeout=60 -- echo "Nimbus is online ..."
/apache-storm-*/bin/storm jar /topology-definition/original-recommender-real-time-service-1.0-SNAPSHOT.jar vn.datnguyen.recommender.TopologyDefinition Recommender-Realtime-Topology