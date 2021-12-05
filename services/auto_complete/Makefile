test:
	docker exec -it zookeeper ./bin/zkCli.sh -server localhost:2181 ls /

init:
	docker-compose up -d --build --scale collector-gathering-service=1

	"--------------------CONFIGURATION HADOOP ENV----------------------"
	docker build -t autocomplete/hbase ./hadoop/base

	"----------------------CREATE ZK NODES-----------------------------"
	docker exec -it zookeeper ./bin/zkCli.sh -server localhost:2181 create /phrases ""
	docker exec -it zookeeper ./bin/zkCli.sh -server localhost:2181 create /phrases/collector ""
	docker exec -it zookeeper ./bin/zkCli.sh -server localhost:2181 create /phrases/collector/last_built_target ""

bulk-request:
	python py_bulk_request.py

shutdown:
	docker-compose down
	docker volume prune -f
