DOCKER_NETWORK = auto_complete_default
ENV_FILE = hadoop/hadoop.env
SEARCH_SETTING_ENV = distributor/search/SETTINGS.env 

test:

	docker-compose up -d zookeeper

	docker build -t autocomplete/search_service_1 ./distributor/search

	docker run --rm --name search_service_1 \
		-d -p 8001:5001 \
		-e SERVICE_PORT=5001 \
		--env-file ${SEARCH_SETTING_ENV} \
		autocomplete/search_service_1


init:
	docker-compose up -d

	$(echo "--------------------CONFIGURATION HADOOP ENV----------------------")
	docker build -t autocomplete/hbase ./hadoop/base

	$(echo "----------------------CREATE ZK NODES-----------------------------")
	docker exec -it zookeeper ./bin/zkCli.sh -server localhost:2181 create /autocomplete ""
	docker exec -it zookeeper ./bin/zkCli.sh -server localhost:2181 create /autocomplete/collector ""
	docker exec -it zookeeper ./bin/zkCli.sh -server localhost:2181 create /autocomplete/collector/last_built_target ""

	docker exec -it zookeeper ./bin/zkCli.sh -server localhost:2181 create /autocomplete/distributor ""
	docker exec -it zookeeper ./bin/zkCli.sh -server localhost:2181 create /autocomplete/distributor/last_built_target ""

	$(echo "----------------------CREATE HDFS BATCHES-----------------------------")
	docker run --network ${DOCKER_NETWORK} --env-file ${ENV_FILE} autocomplete/hbase hadoop fs -mkdir -p /words/kafka_sink/
	docker run --network ${DOCKER_NETWORK} --env-file ${ENV_FILE} autocomplete/hbase hadoop fs -mkdir -p /words/with_weight/
	docker run --network ${DOCKER_NETWORK} --env-file ${ENV_FILE} autocomplete/hbase hadoop fs -mkdir -p /words/with_weight_sorted/
	docker run --network ${DOCKER_NETWORK} --env-file ${ENV_FILE} autocomplete/hbase hadoop fs -mkdir -p /words/tries/

	$(echo "----------------------CREATE SEARCH SERVICES--------------------------")
	docker build -t autocomplete/search_service_1 ./distributor/search
	docker build -t autocomplete/search_service_2 ./distributor/search
	docker build -t autocomplete/search_service_3 ./distributor/search

	docker run --rm --name search_service_1 \
		-d -p 8001:5001 \
		-e SERVICE_PORT=5001 \
		--network ${DOCKER_NETWORK} \
		--env-file ${SEARCH_SETTING_ENV} \
		autocomplete/search_service_1
	
	docker run --rm --name search_service_2 \
		-d -p 8002:5002 \
		-e SERVICE_PORT=5002 \
		--network ${DOCKER_NETWORK} \
		--env-file ${SEARCH_SETTING_ENV} \
		autocomplete/search_service_2
	
	docker run --rm --name search_service_3 \
		-d -p 8003:5003 \
		-e SERVICE_PORT=5003 \
		--network ${DOCKER_NETWORK} \
		--env-file ${SEARCH_SETTING_ENV} \
		autocomplete/search_service_3

migrate:
	docker image prune -a -f 
	docker container prune  -f
	docker build -t autocomplete/weight-processing ./collector/WeightProcessing
	docker run --network ${DOCKER_NETWORK} --env-file ${ENV_FILE} autocomplete/weight-processing

bulk-request:
	python py_bulk_request.py

shutdown:
	docker stop $(shell docker ps -a -q)

	docker-compose down

	docker volume prune  -f
	docker container prune  -f
