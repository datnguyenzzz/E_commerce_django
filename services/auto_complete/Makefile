DOCKER_NETWORK = auto_complete_default
ENV_FILE = hadoop/hadoop.env

test:
	echo "TESTING ZK NODES";
	docker exec -it zookeeper ./bin/zkCli.sh -server localhost:2181 stat /autocomplete
	echo "TESTING HDFS BATCHES";
	docker run --network ${DOCKER_NETWORK} --env-file ${ENV_FILE} autocomplete/hbase hadoop fs -ls /words
	echo "TEST HDFS CAT" 
	docker run --network ${DOCKER_NETWORK} --env-file ${ENV_FILE} autocomplete/hbase hadoop fs -cat /words/1_sink/words-from-client/20211206_1300/words-from-client+0+0000000000+0000000002.avro

init:
	docker-compose up -d --build --scale collector-gathering-service=1

	echo "--------------------CONFIGURATION HADOOP ENV----------------------";
	docker build -t autocomplete/hbase ./hadoop/base

	echo "----------------------CREATE ZK NODES-----------------------------";
	docker exec -it zookeeper ./bin/zkCli.sh -server localhost:2181 create /autocomplete ""
	docker exec -it zookeeper ./bin/zkCli.sh -server localhost:2181 create /autocomplete/collector ""
	docker exec -it zookeeper ./bin/zkCli.sh -server localhost:2181 create /autocomplete/collector/last_built_target ""

	echo "----------------------CREATE HDFS BATCHES-----------------------------";
	docker run --network ${DOCKER_NETWORK} --env-file ${ENV_FILE} autocomplete/hbase hadoop fs -mkdir -p /words/1_sink/
	docker run --network ${DOCKER_NETWORK} --env-file ${ENV_FILE} autocomplete/hbase hadoop fs -mkdir -p /words/2_with_weight/
	docker run --network ${DOCKER_NETWORK} --env-file ${ENV_FILE} autocomplete/hbase hadoop fs -mkdir -p /words/3_with_weight_merged/
	docker run --network ${DOCKER_NETWORK} --env-file ${ENV_FILE} autocomplete/hbase hadoop fs -mkdir -p /words/4_with_weight_ordered/
	docker run --network ${DOCKER_NETWORK} --env-file ${ENV_FILE} autocomplete/hbase hadoop fs -mkdir -p /words/5_tries/

migrate:
	docker build -t autocomplete/weight-processing ./collector/WeightProcessing
	docker run --network ${DOCKER_NETWORK} --env-file ${ENV_FILE} autocomplete/weight-processing

bulk-request:
	python py_bulk_request.py

shutdown:
	docker-compose down
	docker volume prune -f
	docker container prune -f
