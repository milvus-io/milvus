from locust import HttpUser, task, tag, LoadTestShape
import pandas as pd

test_data = pd.read_parquet("/dataset/wikipedia-22-12-en-text/test.parquet")

class MilvusUser(HttpUser):
    host = "http://10.100.36.174:9200"


    @tag('fts')
    @task
    def full_text_search(self):
        search_data = test_data.sample(1)["text"].values[0]
        with self.client.post("/wiki_dataset_benchmark/_search",
                              json={
                                "query": {
                                    "match": {
                                        "text": search_data,
                                    }
                                },
                                "size": 100,
                            },
                              headers={"Content-Type": "application/json"},
                              catch_response=True
                              ) as resp:
            if resp.status_code != 200:
                resp.failure(f"query failed with error {resp.text}")
                print(resp.text)
            else:
                pass



class StagesShape(LoadTestShape):
    """
    A simple load test shape class that has different user and spawn_rate at
    different stages.
    Keyword arguments:
        stages -- A list of dicts, each representing a stage with the following keys:
            duration -- When this many seconds pass the test is advanced to the next stage
            users -- Total user count
            spawn_rate -- Number of users to start/stop per second
            stop -- A boolean that can stop that test at a specific stage
        stop_at_end -- Can be set to stop once all stages have run.
    """

    stages = [
        {"duration": 60, "users": 1, "spawn_rate": 1},
        {"duration": 120, "users": 10, "spawn_rate": 50},
        {"duration": 180, "users": 50, "spawn_rate": 50},
        {"duration": 240, "users": 100, "spawn_rate": 50},
        {"duration": 300, "users": 150, "spawn_rate": 50},
        {"duration": 360, "users": 200, "spawn_rate": 50, "stop": True},
    ]

    def tick(self):
        run_time = self.get_run_time()

        for stage in self.stages:
            if run_time < stage["duration"]:
                tick_data = (stage["users"], stage["spawn_rate"])
                return tick_data

        return None
