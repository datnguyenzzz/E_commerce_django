#! /bin/bash

docker-compose down
docker-compose rm -v -f

docker volume prune  -f
docker container prune  -f