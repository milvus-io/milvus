
import random
from locust import HttpUser, task, between


collection_name = "random_1m_2048_512_ip_sq8"
headers = {'Content-Type': "application/json"}
url = '/collections/%s/vectors' % collection_name
top_k = 2
nq = 1
dim = 512
vectors =  [[random.random() for _ in range(dim)] for _ in range(nq)] 
data = {
    "search":{
        "topk": top_k,
        "vectors": vectors,
        "params": {
            "nprobe": 1
        }
    }
}

class MyUser(HttpUser):
    wait_time = between(0, 0.1)
    host = "http://192.168.1.112:19122"

    @task
    def search(self):
        response = self.client.put(url=url, json=data, headers=headers, timeout=2)
        print(response) 
