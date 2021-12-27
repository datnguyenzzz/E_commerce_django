from kazoo.client import KazooClient
import redis
import pickle
from MyLogger import MyLogger

ZK_LAST_BUILT_TARGET = f'/autocomplete/distributor/last_built_target'

class WordRecommendation:
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
        top_phrases = self._top_phrases_from_cache(phrase)
        if top_phrases is not None:
            self._logger.info(f'Got top phrases from cache: {top_phrases}')
            return top_phrases
        
        return [phrase]
    
    def top_phrases_from_cache(self, phrase):
        cache_key = phrase 
        time_expire = int(os.getenv("CACHE_LIFE_TIME")) * 60 
        self._logger.info(f'Get top phrases from cache with key: {cache_key}')
        
        if self._cache.exists(key):
            pickled_list = self._cache.get(key)
            return pickle.loads(pickled_list)
        
        return None