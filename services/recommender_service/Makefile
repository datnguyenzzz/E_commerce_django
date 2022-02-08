init:
	docker-compose up -d dynamodb kafka redis postgresql dynamodb serviceregistry

testRecommender:
	mvn clean 
	mvn package
	docker-compose up -d zookeeper kafka redis nimbus supervisor ui recommenderrealtimeservice testingrating --scale supervisor=1

testProducer:
	docker exec -it kafka kafka-console-producer.sh --bootstrap-server localhost:9093 --topic ratingCommandForRecommendation

testConsumer:
	docker exec -it kafka kafka-topics.sh --list --bootstrap-server localhost:9093
	docker exec -it kafka kafka-console-consumer.sh --bootstrap-server localhost:9093 --topic ratingCommandForRecommendation --from-beginning

start:
	mvn clean
	mvn install
	docker-compose ps
	docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' prometheus

shutdown:
	docker stop $(shell docker ps -a -q)

	docker-compose down

	docker volume prune  -f
	docker container prune  -f