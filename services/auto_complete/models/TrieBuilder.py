import os
import time
import bisect
import pickle
from kazoo.client import KazooClient, DataWatch

from Trie import Trie
from HdfsClient import HdfsClient
from MyLogger import MyLogger

ZK_LAST_BUILT_FROM_HADOOP = '/autocomplete/collector/last_built_target'
ZK_TO_DISTRIBUTOR = '/autocomplete/distributor/last_built_target'
TRIE_PARTITIONS = int(os.getenv("TRIE_PARTITIONS"))
#ZK_LAST_BUILT_FROM_HADOOP = '/test'

class TrieBuilder:
    def __init__(self):
        self._zk = KazooClient(hosts=f'{os.getenv("ZK_HOST")}:{os.getenv("ZK_PORT")}')
        
        self._hdfsClient = HdfsClient(os.getenv("HADOOP_NAMENODE"), os.getenv("HADOOP_DATANODE"))
        
        self._logger = MyLogger(__name__).logger
    
    def start(self):
        self._zk.start()
        self._logger.info(f'start ZK on {os.getenv("ZK_HOST")}:{os.getenv("ZK_PORT")}')
        data_watch = DataWatch(client=self._zk, path=ZK_LAST_BUILT_FROM_HADOOP, func=self._on_last_built_changed) 
    
    def stop(self):
        self._zk.stop()
        
    def _is_built(self, target_id):
        if (self._zk.exists(ZK_TO_DISTRIBUTOR) is None):
            return False 
        
        next_target_id = self._zk.get(ZK_TO_DISTRIBUTOR)[0].decode()
        return next_target_id == target_id
        
    def _on_last_built_changed(self, data, stat, event=None):
        #data in byte string
        self._logger.info(f"_on_last_built_changed data is {data}")
        
        if data is None:
            return 
        
        #self._build(data)
        self._build(data.decode()) #decode to string
    
    def _init_boundaries(self):
        all_size = ord('z') - ord('a') + 1 
        partitions_size = all_size // TRIE_PARTITIONS + 1
        
        boundaries = [] 
        for i in range(ord('a'),ord('z')+1, partitions_size):
            if i!= ord('a'):
                boundaries.append(chr(i))
            
        boundaries.append('{') 
        
        return boundaries
    
    def _get_trie_hdfs_file(self, target_id, boundary):
        return f'/words/tries/{target_id}/{boundary}'
    
    def _build(self,target_id):
        if not target_id or self._is_built(target_id):
            return False
        
        self._logger.info(self._hdfsClient.list("/words/with_weight_sorted/" + target_id))
        
        trie_list = [Trie() for _ in range(TRIE_PARTITIONS)]
        
        boundaries = self._init_boundaries()
        
        self._create_trie(target_id, trie_list, boundaries)
                
        for trie,boundary in zip(trie_list,enumerate(boundaries)):
            print(trie, boundary[0], boundary[1])
            #store to local and move to hdfs 
            local_trie_file = "trie.dat"
            pickle.dump(trie, open(local_trie_file,'wb'))
            
            trie_hdfs_file = self._get_trie_hdfs_file(target_id, boundary[1])
            self._hdfsClient.upload_to_hdfs(local_trie_file, trie_hdfs_file)
            
            #register hdfs trie file locator to zk 
            self._register_trie_locator(target_id, boundary[0], boundary[1], trie_hdfs_file)
            
        self._register_last_build_id(target_id)
            
        return True
    
    def _create_trie(self,target_id, trie_list, bounded):
        
        hdfs_source = f'/words/with_weight_sorted/{target_id}/part-r-00000'
        self._logger.info(f'HDFS source {hdfs_source}')
        with self._hdfsClient.get_stream(hdfs_source) as stream:
            for line in stream.iter_lines():
                if not line:
                    continue 
                
                # weight - word
                word = line.decode("utf-8").split("\t", maxsplit=1)[1]
                pos = bisect.bisect_right(bounded, word)
                trie_list[pos].add_word(word)
                print(pos,"----",word)
                self._logger.info(f'Adding word: {word}')
        
    def _register_trie_locator(self, target_id, boundary_id, boundary, trie_hdfs_file):
        base_zk_path = f'/autocomplete/distributor/{target_id}/{boundary}'
        #trie hdfs file locator
        self._zk.ensure_path(f'{base_zk_path}/trie_hdfs_locator')
        self._zk.set(f'{base_zk_path}/trie_hdfs_locator', trie_hdfs_file.encode())
        
        #server host 
        self._zk.ensure_path(f'{base_zk_path}/server_host')
        server_host = f'search_service_{boundary_id+1}:{boundary_id+5001}'
        self._zk.set(f'{base_zk_path}/server_host', server_host.encode())
    
    def _register_last_build_id(self, target_id):
        base_zk_path = ZK_TO_DISTRIBUTOR
        self._zk.ensure_path(base_zk_path)
        self._zk.set(base_zk_path, target_id.encode())
    
def test_trie_model():
    trie = Trie() 
    trie.add_word("abc")
    trie.add_word("aac")
    trie.add_word("aab")
    trie.add_word("abd")
    trie.add_word("acd")
    trie.add_word("ace")
    trie.add_word("acc")    
    print(trie.get_top_popular("ab"))

if __name__ == "__main__":
    #test_trie_model()
    trie_builder = TrieBuilder() 
    trie_builder.start()
    
    while True:
        time.sleep(5)