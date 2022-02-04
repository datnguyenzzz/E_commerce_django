init:
	docker-compose up -d dynamodb kafka redis postgresql dynamodb serviceregistry

testRecommender:
	docker-compose up -d zookeeper nimbus supervisor ui --scale supervisor=3
	docker build -t wurstmeister/storm ./storm/storm-base
	docker build -t wurstmeister/storm-nimbus ./storm/storm-nimbus
	docker build -t wurstmeister/storm-supervisor ./storm/storm-supervisor
	docker build -t wurstmeister/storm-ui ./storm/storm-ui

testConsumer:
	docker exec -it kafka kafka-topics.sh --list --bootstrap-server localhost:9093
	docker exec -it kafka kafka-console-consumer.sh --bootstrap-server localhost:9093 --topic ratingCommandFromEventSource --from-beginning

start:
	mvn clean
	mvn install
	docker-compose up -d --scale commandrating=1 --scale queryrating=1
##	docker-compose up -d
	docker-compose ps
	docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' prometheus

shutdown:
	docker stop $(shell docker ps -a -q)

	docker-compose down

	docker volume prune  -f
	docker container prune  -f