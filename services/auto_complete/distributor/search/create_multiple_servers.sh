#!/bin/bash

TRIE_PATITIONS=5

for i in {1..$TRIE_PATITIONS}
do
   	docker build -t autocomplete/search_service_$i .

	docker run -d -p 800$i:500$i \
		-e SERVICE_PORT=500$i \
		-e ZK_HOST=zookeeper \
		-e ZK_PORT=2181 \
		-e HADOOP_NAMENODE=hdfs-namenode \
		-e HADOOP_DATANODE=hdfs-datanode \
		autocomplete/search_service_$i
done