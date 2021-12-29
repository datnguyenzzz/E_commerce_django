from threading import Thread
import sys
import requests
from queue import Queue
import random 
import string
import time

url = "http://localhost:8000/top-phrases"
concurrent = 5
q = Queue(concurrent)

def gen_data():
    chars=string.ascii_uppercase + string.digits
    size = random.randint(3, 10)
    phrase = ''.join(random.choice(chars) for _ in range(size))
    
    return phrase
        

def doWork():
    while True:
        data_str = q.get()
        URI = f'{url}/{data_str}'
        
        start_time = time.time()
        print(f"START REQUEST WITH PHRASE = {data_str}")
        r = requests.get(URI)
        end_time = time.time()
        
        print(f"RESULT FROM REQUEST = {r.text} within {end_time - start_time} sec")
        q.task_done()

def main():
    for i in range(concurrent):
        t = Thread(target=doWork)
        t.daemon = True
        t.start()
    try:
        MAX_REQ = 2000
        old_data = ""
        for i in range(MAX_REQ):
            times = random.randint(1,5)
            data_str = gen_data()
            
            choose = random.choice([0,1])
            if choose==0:
                previous = random.randint(0, min(5,len(old_data)))
            else:
                previous = 0
                
            data_str = "".join([old_data[:previous], data_str])
            for _ in range(times):
                q.put(data_str)
            
            old_data = data_str
        q.join()
    except KeyboardInterrupt:
        sys.exit(1)

if __name__ == "__main__":        
    #test() 
    main()
    