import os
import time
from kazoo.client import KazooClient, DataWatch

from Trie import Trie
from HdfsClient import HdfsClient
from MyLogger import MyLogger

ZK_LAST_BUILT_FROM_HADOOP = '/autocomplete/collector/last_built_target'
ZK_TO_DISTRIBUTOR = '/autocomplete/distributor/from_last_collector'
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
    
    def _build(self,target_id):
        if not target_id or self._is_built(target_id):
            return False
        
        self._logger.info(self._hdfsClient.list("/words/with_weight_sorted/" + target_id))
        
        trie = self._create_trie(target_id)
        
        return True
    
    def _create_trie(self,target_id):
        trie = Trie()
        
        hdfs_source = f'/words/with_weight_sorted/{target_id}/part-r-00000'
        self._logger.info(f'HDFS source {hdfs_source}')
        with self._hdfsClient.get_stream(hdfs_source) as stream:
            for line in stream.iter_lines():
                if not line:
                    continue 
                
                # weight - word
                word = line.decode("utf-8").split("\t", maxsplit=1)[1]
                trie.add_word(word)
                self._logger.info(f'Adding word: {word}')
        
        return trie

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