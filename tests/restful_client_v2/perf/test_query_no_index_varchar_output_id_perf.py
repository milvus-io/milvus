from locust import HttpUser, task, LoadTestShape


class MilvusUser(HttpUser):
    host = "http://10.104.33.50:19530"
    id_to_get = [i for i in range(100)]

    @task
    def query(self):
        payload = {"collectionName": "test_restful_perf",
                   "filter": 'text_no_index like "99%"',
                   "outputFields": ["id"],
                   "limit": 100
                   }
        with self.client.post("/v2/vectordb/entities/query",
                              json=payload,
                              headers={"Content-Type": "application/json", "Authorization": "Bearer root:Milvus"},
                              catch_response=True
                              ) as resp:
            if resp.status_code != 200 or resp.json()["code"] != 200 or len(resp.json()["data"]) == 0:
                resp.failure(f"query failed with error {resp.text} for payload {payload}")


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
        {"duration": 120, "users": 10, "spawn_rate": 10},
        {"duration": 180, "users": 50, "spawn_rate": 10},
        {"duration": 240, "users": 100, "spawn_rate": 10},
        {"duration": 300, "users": 30, "spawn_rate": 10},
        {"duration": 360, "users": 10, "spawn_rate": 10},
        {"duration": 420, "users": 1, "spawn_rate": 1, "stop": True},
    ]

    def tick(self):
        run_time = self.get_run_time()

        for stage in self.stages:
            if run_time < stage["duration"]:
                tick_data = (stage["users"], stage["spawn_rate"])
                return tick_data

        return None
