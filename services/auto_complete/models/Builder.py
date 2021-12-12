import os
import time
import logging
from kazoo.client import KazooClient, DataWatch

from Trie import Trie

#ZK_LAST_BUILT_FROM_HADOOP = '/autocomplete/collector/last_built_target'
ZK_LAST_BUILT_FROM_HADOOP = '/test'

class TrieBuilder:
    def __init__(self):
        print(f'===============ZK HOST : {os.getenv("ZK_HOST")}:{os.getenv("ZK_PORT")} ==================')
        self._zk = KazooClient(hosts=f'{os.getenv("ZK_HOST")}:{os.getenv("ZK_PORT")}')
        
        self._logger = logging.getLogger(__name__)
        log_lvl_name = logging.getLevelName(os.getenv("LOG_LEVEL","INFO"))
        self._logger.setLevel(log_lvl_name)
        log_handler = logging.StreamHandler() 
        log_handler.setLevel(log_lvl_name)
        self._logger.addHandler(log_handler)
    
    def start(self):
        print("===============START ZOOKEEPER CONNECT ==================")
        self._zk.start()
        data_watch = DataWatch(client=self._zk, path=ZK_LAST_BUILT_FROM_HADOOP, func=self._on_last_built_changed) 
    
    def stop(self):
        self._zk.stop()
        
    def _on_last_built_changed(self, data, stat, event=None):
        #data in byte string
        self._logger.debug(f"_on_last_built_changed data is {data}")
        
        if data is None:
            return 
        
        self._build(data)
    
    def _build(self,data):
        if not data:
            return False
        
        self._zk.create("/test_res", data)
        
        return True

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