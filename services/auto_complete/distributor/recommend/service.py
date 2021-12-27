from kazoo.client import KazooClient
import redis
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
        self._distributed_cache_enabled = True \
            if os.getenv("DISTRIBUTED_CACHE_ENABLED") == 'true' \
            else False
          
    def start(self):
        self._zk.start()
        
    def stop(self):
        self._zk.stop()
        
    def top_phrases_for(self, phrase):
        return [phrase]