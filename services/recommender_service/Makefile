DOCKER_NETWORK = recommender_service_default
hadoop_branch = 2.0.0-hadoop2.7.4-java8
hbase_branch = 1.0.0-hbase1.2.6

init:
	docker-compose up -d dynamodb kafka redis postgresql dynamodb serviceregistry

testRecommender:
	mvn clean
	mvn install
#	docker build -t bde2020/hbase-base:$(hbase_branch) ./hadoop/hbase
#   docker build -t bde2020/hbase-master:$(hbase_branch) ./hadoop/hmaster
#	docker build -t bde2020/hbase-regionserver:$(hbase_branch) ./hadoop/hregionserver
#	docker build -t bde2020/hbase-pseudo-distributed:$(hbase_branch) ./hadoop/pseudo-distributed
	docker-compose -f docker-compose-hbase-local-distributed.yml -f docker-compose-test-recommender.yml up -d --scale supervisor=1
	docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' prometheus

testProducer:
	docker exec -it kafka kafka-console-producer.sh --bootstrap-server localhost:9093 --topic ratingCommandForRecommendation

testConsumer:
	docker exec -it kafka kafka-topics.sh --list --bootstrap-server localhost:9093
	docker exec -it kafka kafka-console-consumer.sh --bootstrap-server localhost:9093 --topic ratingCommandForRecommendation --from-beginning

start:
	mvn clean
	mvn install
	docker build -t bde2020/hbase-base:latest ./hbase
	docker build -t bde2020/hbase-master:latest ./hmaster
	docker build -t bde2020/hbase-regionserver:latest ./hregionserver
	docker build -t bde2020/hbase-standalone:latest ./pseudo-distributed
	docker-compose -f docker-compose-extra-services.yml -f docker-compose-hbase-local-distributed.yml -f docker-compose-main-services up -d --scale supervisor=1 --scale commandrating=1 --scale queryrating=1
	docker-compose ps
	docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' prometheus

shutdown:
	docker stop $(shell docker ps -a -q)

	docker-compose  -f docker-compose-hbase-local-distributed.yml -f docker-compose-test-recommender.yml down

	docker volume prune  -f
	docker container prune  -f