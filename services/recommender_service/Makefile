testConsumer:
	docker exec -it kafka kafka-topics.sh --list --bootstrap-server localhost:9093
	docker exec -it kafka kafka-console-consumer.sh --bootstrap-server localhost:9093 --topic ratingCommandFromEventSource --from-beginning

start:
	mvn clean install
	docker-compose up -d
	docker-compose ps

shutdown:
	docker stop $(shell docker ps -a -q)

	docker-compose down

	docker volume prune  -f
	docker container prune  -f