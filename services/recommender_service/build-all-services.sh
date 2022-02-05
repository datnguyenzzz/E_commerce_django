#! /bin/bash 

docker-compose up -d zookeeper nimbus supervisor ui --scale supervisor=3
docker build -t wurstmeister/storm ./storm/storm-base
docker build -t wurstmeister/storm-nimbus ./storm/storm-nimbus
docker build -t wurstmeister/storm-supervisor ./storm/storm-supervisor
docker build -t wurstmeister/storm-ui ./storm/storm-ui
containerId=$(docker inspect -f '{{.Id}}' recommender_service-nimbus-1)
docker cp ./recommender-real-time-service/target/recommender-real-time-service-1.0-SNAPSHOT.jar \
        ${containerId}:/storm-topology/topo.jar