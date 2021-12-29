import logging
import os

class MyLogger:
    def __init__(self, name):
        _LOG_LEVEL = os.getenv("LOG_LEVEL","INFO")
        _LOG_FILE = f'{_LOG_LEVEL}.log'
        _LOG_LEVEL_NAME = logging.getLevelName(_LOG_LEVEL)
        
        logging.basicConfig(filename=_LOG_FILE,
                            filemode='a',
                            format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                            datefmt='%H:%M:%S',
                            level=_LOG_LEVEL_NAME)
        
        self._logger = logging.getLogger(name)
        self._logger.setLevel(_LOG_LEVEL_NAME)
        log_handler = logging.StreamHandler() 
        log_handler.setLevel(_LOG_LEVEL_NAME)
        self._logger.addHandler(log_handler)
    
    @property
    def logger(self):
        return self._logger