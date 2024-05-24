from locust import HttpUser, task, events, LoadTestShape
import random
import pandas as pd


@events.init_command_line_parser.add_listener
def _(parser):
    parser.add_argument("--filter", type=str, env_var="LOCUST_FILTER", default="", help="filter expr")


@events.test_start.add_listener
def _(environment, **kw):
    print(f"Custom argument supplied: {environment.parsed_options.filter}")


class MilvusUser(HttpUser):
    host = "http://10.104.13.233:19530"
    test_data_file_name = "/root/dataset/laion_with_scalar_medium_10m/test.parquet"
    df = pd.read_parquet(test_data_file_name)
    vectors_to_search = df["emb"].tolist()
    recall_list = []
    recall = 0

    def on_start(self):
        print("X. Here's where you would put things you want to run the first time a User is started")
        expr = self.environment.parsed_options.filter
        ascii_codes = [str(ord(char)) for char in expr]
        expr_ascii = "".join(ascii_codes)
        if expr_ascii:
            ground_truth_file_name = f"/root/dataset/laion_with_scalar_medium_10m/neighbors-{expr_ascii}.parquet"
        else:
            ground_truth_file_name = f"/root/dataset/laion_with_scalar_medium_10m/neighbors.parquet"
        df_neighbors = pd.read_parquet(ground_truth_file_name)
        self.gt = df_neighbors["neighbors_id"].tolist()

    @task
    def search(self):
        filter = self.environment.parsed_options.filter
        # print(filter)
        random_id = random.randint(0, len(self.vectors_to_search) - 1)
        # print([self.vectors_to_search[random_id].tolist()])
        data = [self.vectors_to_search[random_id].tolist()]
        with self.client.post("/v2/vectordb/entities/search",
                              json={"collectionName": "test_restful_perf",
                                    "annsField": "emb",
                                    "data": data,
                                    "outputFields": ["id"],
                                    "filter": filter,
                                    "limit": 1000
                                    },
                              headers={"Content-Type": "application/json", "Authorization": "Bearer root:Milvus"},
                              catch_response=True
                              ) as resp:
            if resp.status_code != 200 or resp.json()["code"] != 200:
                resp.failure(f"search failed with error {resp.text}")
            else:
                # compute recall
                result_ids = [item["id"] for item in resp.json()["data"]]
                true_ids = [item for item in self.gt[random_id]]
                tmp = set(true_ids).intersection(set(result_ids))
                self.recall = len(tmp) / len(result_ids)
                self.recall_list.append(self.recall)

    def on_stop(self):
        # this is a good place to clean up/release any user-specific test data
        print(f"current recall is {self.recall}, "
              f"avg recall is {sum(self.recall_list) / len(self.recall_list)}, "
              f"max recall is {max(self.recall_list)}, "
              f"min recall is {min(self.recall_list)}")


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
        {"duration": 120, "users": 2, "spawn_rate": 1},
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
