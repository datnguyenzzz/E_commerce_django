import os
from kazoo.client import KazooClient, DataWatch

from MyLogger import MyLogger
from HdfsClient import HdfsClient

LAST_BUILT_TARGET = '/autocomplete/distributor/last_built_target'
TRIE_PARTITION = os.getenv("TRIE_PARTITION")

class Servers:
    def __init__(self, partition):
        
        self._partition = partition
        #logger
        self._logger = MyLogger(__name__).logger
        #hdfs client 
        self._hdfs_client = HdfsClient(os.getenv("HADOOP_NAMENODE"), os.getenv("HADOOP_DATANODE"))
        #zookeeper 
        self._zk = KazooClient(hosts=f'{os.getenv("ZK_HOST")}:{os.getenv("ZK_PORT")}')
        
    def start(self):
        self._zk.start()
        _ = DataWatch(client = self._zk, path=LAST_BUILT_TARGET, func=self._on_last_built_target_changed)

    def _on_last_built_target_changed(self, data, stat, event=None):
        pass