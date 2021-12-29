import os
import pickle
from collections import deque
from kazoo.client import KazooClient, DataWatch

from MyLogger import MyLogger
from HdfsClient import HdfsClient

ZK_DISTRIBUTOR_BASE = '/autocomplete/distributor'
LAST_BUILT_TARGET = f'{ZK_DISTRIBUTOR_BASE}/last_built_target'
TRIE_PARTITIONS = int(os.getenv("TRIE_PARTITIONS"))
ROLLBACK_QUEUE = int(os.getenv("ROLLBACK_QUEUE"))

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
        self._trie = None
        self._rollback = deque()
        #logger
        self._logger = MyLogger(__name__).logger
        #hdfs client 
        self._hdfs_client = HdfsClient(os.getenv("HADOOP_NAMENODE"))
        #zookeeper 
        self._zk = KazooClient(hosts=f'{os.getenv("ZK_HOST")}:{os.getenv("ZK_PORT")}')
        
    def _set_trie(self, trie):
        self._trie = trie
        
    def start(self):
        self._logger.info(f"Start server bind to partion {self._partition}")
        self._zk.start()
        datawatch = DataWatch(client = self._zk, path=LAST_BUILT_TARGET, func=self._on_last_built_target_changed)
    
    def _load_trie(self, target_id):
        trie_locator = f'{ZK_DISTRIBUTOR_BASE}/{target_id}/{self._partition}/trie_hdfs_locator'
        self._logger.info(f'Try to get trie locator from: {trie_locator}')
        
        trie_async_obj = self._zk.get_async(trie_locator)
        trie_hdfs_path = trie_async_obj.get(block=True)[0].decode()
        self._logger.info(f'Trie hdfs HDFS path: {trie_hdfs_path}')
        
        trie_local_path = 'trie.dat'
        
        self._hdfs_client.download(trie_hdfs_path, trie_local_path)
        
        with open(trie_local_path, 'rb') as f:
            trie_obj = pickle.load(f)
            self._logger.info(f'trie loaded successfully from hdfs: {trie_obj}')
            self._set_trie(trie_obj)
    
    def _migrate(self, target_id):
        if not target_id:
            return False 
        
        self._logger.info(f'Start load trie from target: {target_id}')
        self._load_trie(target_id)
        
        return True

    def _on_last_built_target_changed(self, data, stat, event=None):
        self._logger.info(f'last built target changed: {data}')
        if data is None:
            return 
        
        self._migrate(data.decode())