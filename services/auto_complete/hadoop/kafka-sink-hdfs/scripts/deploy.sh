#!/bin/sh

echo "Creating Kafka sink to HDFS ...."
/etc/confluent/docker/run & \

echo "Waiting for Kafka Connect to start listening on $CONNECT_REST_ADVERTISED_HOST_NAME"
while [ $(curl -s -o /dev/null -w %{http_code} http://$CONNECT_REST_ADVERTISED_HOST_NAME:$CONNECT_REST_PORT/connectors) -ne 200 ]; do 
    echo -e $(date) " Kafka Connect listener HTTP state: " $(curl -s -o /dev/null -w %{http_code} http://$CONNECT_REST_ADVERTISED_HOST_NAME:$CONNECT_REST_PORT/connectors) " (waiting for 200)"
    sleep 2
done

nc -vz $CONNECT_REST_ADVERTISED_HOST_NAME $CONNECT_REST_PORT
echo -e "\n--\n+> DEPLOYING HDFS SINK....."
chmod +x /scripts/hdfs-sink.sh
/scripts/hdfs-sink.sh 
sleep infinity
