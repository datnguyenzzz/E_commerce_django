import os
import pickle
from kazoo.client import KazooClient, DataWatch

from MyLogger import MyLogger
from HdfsClient import HdfsClient

LAST_BUILT_TARGET = '/autocomplete/distributor/last_built_target'
TRIE_PARTITION = os.getenv("TRIE_PARTITION")

def _init_boundaries():
    all_size = ord('z') - ord('a') + 1 
    partitions_size = all_size // TRIE_PARTITIONS + 1
        
    boundaries = [] 
    for i in range(ord('a'),ord('z')+1, partitions_size):
        if i!= ord('a'):
            boundaries.append(chr(i))
            
    boundaries.append('{') 
        
    return boundaries  

class Servers:
    _partitions = _init_boundaries()
    def __init__(self, partition):
        self._partition = self._partitions[partition]
        #logger
        self._logger = MyLogger(__name__).logger
        #hdfs client 
        self._hdfs_client = HdfsClient(os.getenv("HADOOP_NAMENODE"), os.getenv("HADOOP_DATANODE"))
        #zookeeper 
        self._zk = KazooClient(hosts=f'{os.getenv("ZK_HOST")}:{os.getenv("ZK_PORT")}')
        
    def start(self):
        self._zk.start()
        _ = DataWatch(client = self._zk, path=LAST_BUILT_TARGET, func=self._on_last_built_target_changed)
    
    def _load_trie(self, target_id):
        pass

    def _on_last_built_target_changed(self, data, stat, event=None):
        self._logger.info(f'last built target changed: {data}')
        if data is None:
            return
        target_id = data.decode()