DOCKER_NETWORK = auto_complete_default
ENV_FILE = hadoop/hadoop.env
SEARCH_SETTING_ENV = distributor/search/SETTINGS.env 

TRIE_PARTITIONS = 1 2 3 

test_recommend:
	docker-compose up -d recommend-service

test:
	docker-compose up -d zookeeper
	docker-compose up -d hdfs-namenode
	docker-compose up -d recommend-service

	$(foreach var,$(TRIE_PARTITIONS),docker build -t autocomplete/search-service-$(var) ./distributor/search &&) echo.
	$(foreach var,$(TRIE_PARTITIONS),docker run \
		--rm --name search-service-$(var) \
		-d -p 800$(var):500$(var) \
		-e SERVICE_PORT=500$(var) \
		--network ${DOCKER_NETWORK} \
		--env-file ${SEARCH_SETTING_ENV} \
		autocomplete/search-service-$(var) &&) echo.

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
	$(foreach var,$(TRIE_PARTITIONS),docker build -t autocomplete/search-service-$(var) ./distributor/search &&) echo.
	$(foreach var,$(TRIE_PARTITIONS),docker run \
		--rm --name search-service-$(var) -d \
		-e SERVICE_PORT=500$(var) \
		--network ${DOCKER_NETWORK} \
		--env-file ${SEARCH_SETTING_ENV} \
		autocomplete/search-service-$(var) &&) echo.

migrate:
	docker image prune -a -f 
	docker container prune  -f
	docker build -t autocomplete/weight-processing ./collector/WeightProcessing
	docker run --name Weight_Process --network ${DOCKER_NETWORK} --env-file ${ENV_FILE} autocomplete/weight-processing

init_data:
	python initial_data.py

bulk_request:
	python bulk_request.py

shutdown:
	docker stop $(shell docker ps -a -q)

	docker-compose down

	docker volume prune  -f
	docker container prune  -f
