import os
from kazoo.client import KazooClient

from MyLogger import MyLogger
from HdfsClient import HdfsClient

CURRENT_TARGET = '/autocomplete/distributor/last_built_target'

class Servers:
    def __init__(self):
        #logger
        self._logger = MyLogger(__name__).logger
        #hdfs client 
        self._hdfs_client = HdfsClient(os.getenv("HADOOP_NAMENODE"), os.getenv("HADOOP_DATANODE"))
        #zookeeper 
        self._zk = KazooClient(hosts=f'{os.getenv("ZK_HOST")}:{os.getenv("ZK_PORT")}')