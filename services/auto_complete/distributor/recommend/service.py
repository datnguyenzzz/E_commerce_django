import os
import redis
import pickle
import bisect
import requests
import sys

from kazoo.client import KazooClient
from kazoo.exceptions import ConnectionLossException
from kazoo.exceptions import NoAuthException

from MyLogger import MyLogger

ZK_DITRIBUTOR_BASE = f'/autocomplete/distributor'
ZK_LAST_BUILT_TARGET = f'{ZK_DITRIBUTOR_BASE}/last_built_target'
TRIE_PARTITIONS = int(os.getenv("TRIE_PARTITIONS"))
CACHE_LIFE_TIME = int(os.getenv("CACHE_LIFE_TIME"))

def _init_boundaries():
    all_size = ord('z') - ord('a') + 1 
    partitions_size = all_size // TRIE_PARTITIONS + 1
        
    boundaries = [] 
    for i in range(ord('a'),ord('z')+1, partitions_size):
        if i!= ord('a'):
            boundaries.append(chr(i))
            
    boundaries.append('{') 
        
    return boundaries  

class WordRecommendation:
    
    _trie_partitions = _init_boundaries()
    
    def __init__(self):
        #logger
        self._logger = MyLogger(__name__).logger
        #zookeeper
        self._zk = KazooClient(hosts=f'{os.getenv("ZK_HOST")}:{os.getenv("ZK_PORT")}')
        #cache 
        self._cache = redis.Redis(host=os.getenv("CACHE_HOST"), port=os.getenv("CACHE_PORT"), db=0)

          
    def start(self):
        self._zk.start()
        
    def stop(self):
        self._zk.stop()
        
    def top_phrases_for(self, phrase):
        top_phrases_from_cache = self._top_phrases_from_cache(phrase)
        if top_phrases_from_cache is not None:
            self._logger.info(f'Got top phrases from cache: {top_phrases_from_cache}')
            return top_phrases_from_cache
        
        top_phrase = self._response_from_server_host_for(phrase)
        
        self._insert_to_cache(phrase, top_phrase)
        return top_phrase
    
    def _response_from_server_host_for(self,phrase):
        
        top_phrases = None
        pos = bisect.bisect_right(self._trie_partitions, phrase)
        
        if (self._zk.exists(ZK_LAST_BUILT_TARGET) is None):
            return phrase
        
        target_id = self._zk.get(ZK_LAST_BUILT_TARGET)[0].decode()
        if not target_id:
            return phrase
        
        server_host_async = self._zk.get_async(f'{ZK_DITRIBUTOR_BASE}/{target_id}/{self._trie_partitions[pos]}/server_host')
        
        host = server_host_async.get(block=True)[0].decode()
        self._logger.info(f'Request to server: http://{host}/search-services/{phrase}')
        r = requests.get(f'http://{host}/search-services/{phrase}')
        self._logger.info(f'Response from server: {r.text}')
        
        return r.text
    
    def _top_phrases_from_cache(self, phrase):
        
        cache_key = phrase 
        self._logger.info(f'Get top phrases from cache with key: {cache_key}')
        
        if self._cache.exists(cache_key):
            pickled_list = self._cache.get(cache_key)
            result = pickle.loads(pickled_list)
            return result
        
        return None
    
    def _insert_to_cache(self, key, value):
        time_expire = CACHE_LIFE_TIME * 60
        self._cache.set(key, pickle.dumps(value), ex=time_expire)