init:
	docker-compose up -d dynamodb kafka redis postgresql dynamodb serviceregistry

testQueryService:
	mvn clean 
	docker-compose up -d dynamodb 
	mvn package

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