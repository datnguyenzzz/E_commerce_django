from threading import Thread
import sys
import requests
from queue import Queue
import random 
import string
import time
import uuid
import json

url = "http://localhost:3456/api/v/1.0.0/testing/item"
headers = {
    'content-type': 'application/json'}
concurrent = 5
ALL_SENT = 0
q = Queue(concurrent)

def genData():
    postData = {}
    postData['clientId'] = str(uuid.uuid4())
    postData['itemId'] = str(uuid.uuid4())
    postData['property1'] = random.randint(0,20)
    postData['property2'] = random.randint(0,20)
    postData['property3'] = random.randint(0,20)
    return postData

def doWork():
    global ALL_SENT
    while True:
        data = q.get()
        
        x = requests.post(url, json = data, headers = headers)
        ALL_SENT += 1
        print(f"REQUEST SENT - {ALL_SENT} : Post request with data = {data} with result = {x.text}")
        q.task_done()

def main():
    for i in range(concurrent):
        t = Thread(target=doWork)
        t.daemon = True
        t.start()
    try:
        MAX_REQ = 100
        old_data = ""
        for i in range(MAX_REQ):
            q.put(genData())
        q.join()
    except KeyboardInterrupt:
        sys.exit(1)

if __name__ == "__main__":        
    #test() 
    main()
    