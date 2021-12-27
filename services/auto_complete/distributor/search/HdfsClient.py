from hdfs import InsecureClient

class HdfsClient:
    def __init__(self, namenode, datanode):
        self._namenode = namenode 
        self._datanode = datanode
        self._client = InsecureClient(f'http://{self._namenode}:9870')
        
    def download(self, hdfs_path, local_path):
        self._client.download(hdfs_path, local_path, overwrite=True)