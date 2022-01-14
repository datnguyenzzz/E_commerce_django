init:
	mvn clean
	mvn install
	docker-compose up -d
	docker-compose ps

shutdown:
	docker stop $(shell docker ps -a -q)

	docker-compose down

	docker volume prune  -f
	docker container prune  -f