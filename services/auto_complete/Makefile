DOCKER_NETWORK = auto_complete_default
ENV_FILE = hadoop/hadoop.env

test:
	echo "TESTING ZK NODES";
	docker exec -it zookeeper ./bin/zkCli.sh -server localhost:2181 get /autocomplete/collector/last_built_target
	echo "TESTING HDFS BATCHES";
	docker run --network ${DOCKER_NETWORK} --env-file ${ENV_FILE} autocomplete/hbase hadoop fs -ls /words

init:
	docker-compose up -d --build --scale collector-gathering-service=1

	echo "--------------------CONFIGURATION HADOOP ENV----------------------";
	docker build -t autocomplete/hbase ./hadoop/base

	echo "----------------------CREATE ZK NODES-----------------------------";
	docker exec -it zookeeper ./bin/zkCli.sh -server localhost:2181 create /autocomplete ""
	docker exec -it zookeeper ./bin/zkCli.sh -server localhost:2181 create /autocomplete/collector ""
	docker exec -it zookeeper ./bin/zkCli.sh -server localhost:2181 create /autocomplete/collector/last_built_target ""

	echo "----------------------CREATE HDFS BATCHES-----------------------------";
	docker run --network ${DOCKER_NETWORK} --env-file ${ENV_FILE} autocomplete/hbase hadoop fs -mkdir -p /words/kafka_sink/
	docker run --network ${DOCKER_NETWORK} --env-file ${ENV_FILE} autocomplete/hbase hadoop fs -mkdir -p /words/with_weight/
	docker run --network ${DOCKER_NETWORK} --env-file ${ENV_FILE} autocomplete/hbase hadoop fs -mkdir -p /words/with_weight_sorted/
	docker run --network ${DOCKER_NETWORK} --env-file ${ENV_FILE} autocomplete/hbase hadoop fs -mkdir -p /words/tries/


migrate:
	docker build -t autocomplete/weight-processing ./collector/WeightProcessing
	docker run --network ${DOCKER_NETWORK} --env-file ${ENV_FILE} autocomplete/weight-processing
	docker image prune -a -f
	docker container prune -f

bulk-request:
	python py_bulk_request.py

shutdown:
	docker-compose down
	docker volume prune  -f
	docker container prune  -f
