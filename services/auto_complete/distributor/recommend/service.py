from kazoo.client import KazooClient
from MyLogger import MyLogger

ZK_LAST_BUILT_TARGET = f'/autocomplete/distributor/last_built_target'

class WordRecommendation:
    def __init__(self):
        #logger
        self._logger = MyLogger(__name__).logger
        #zookeeper
        self._zk = KazooClient(hosts=f'{os.getenv("ZK_HOST")}:{os.getenv("ZK_PORT")}')
        
    def start(self):
        self._zk.start()