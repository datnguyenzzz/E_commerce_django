from hdfs import InsecureClient
import requests

from MyLogger import MyLogger

class HdfsClientGetStream:
    def __init__(self, request_path):
        self._data = requests.get(request_path, stream=True)
    
    def __enter__(self):
        return self._data.__enter__()
    
    def __exit__(self, type, value, traceback):
	    self._data.__exit__(type, value, traceback)

class HdfsClient:
    def __init__(self, namenode, datanode):
        self._namenode = namenode 
        self._datanode = datanode
        self._client = InsecureClient(f'http://{self._namenode}:9870')
        
        self._logger = MyLogger(__name__).logger
        
    def list(self, path):
        return self._client.list(path) 
    
    def upload_to_hdfs(self, local_path, hdfs_path):
        self._logger.info(f'Upload local path {local_path} to {hdfs_path}')
        
        URI = f'http://{self._namenode}:9870/webhdfs/v1{hdfs_path}?op=CREATE&overwrite=true'
        
        self._logger.info(f'Used URI for uploading {URI}')
        
        with open(local_path, 'rb') as f:
            r = requests.put(URI, data=f)
            self._logger.debug(f'Upload result {r.content}')
            
    def get_stream(self, hdfs_path):
        request_path = f'http://{self._datanode}:9864/webhdfs/v1{hdfs_path}?op=OPEN&namenoderpcaddress={self._namenode}:9000&offset=0'
        return HdfsClientGetStream(request_path)