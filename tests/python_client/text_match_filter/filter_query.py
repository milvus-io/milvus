from locust import HttpUser, task, events, LoadTestShape
import random
import pandas as pd
import re
from datetime import datetime
import uuid

def convert_numbers_to_quoted_strings(text):
    result = re.sub(r'\d+', lambda x: f"'{x.group()}'", text)
    return result

@events.init_command_line_parser.add_listener
def _(parser):
    parser.add_argument("--filter_op", type=str, env_var="LOCUST_FILTER", default="contains", help="filter op")


@events.test_start.add_listener
def _(environment, **kw):
    print(f"Custom argument supplied: {environment.parsed_options.filter_op}")


class MilvusUser(HttpUser):
    host = "http://10.104.18.39:19530"
    filter = ""
    gt = []
    recall_list = []
    ts_list = []
    recall = 0

    def on_start(self):
        # print("X. Here's where you would put things you want to run the first time a User is started")
        filter_op = self.environment.parsed_options.filter_op
        ground_truth_file_name = f"test.parquet"
        df = pd.read_parquet(ground_truth_file_name)
        data = df.query(f"filter == '{filter_op}'")
        filter_value = data["value"].tolist()[0].tolist()
        if filter_op == "contains":
            filter_value = filter_value[0]
            if isinstance(filter_value, str):
                filter_value = f"'{filter_value}'"
            self.filter = f"array_contains({filter_op}, {filter_value})"
        if filter_op == "contains_any":
            self.filter = f"array_contains_any({filter_op}, {filter_value})"
        if filter_op == "contains_all":
            self.filter = f"array_contains_all({filter_op}, {filter_value})"
        if filter_op == "equals":
            self.filter = f"{filter_op} == {filter_value}"
        self.gt = data["target_id"].tolist()[0].tolist()
    @task
    def query(self):
        with self.client.post("/v2/vectordb/entities/query",
                              json={"collectionName": "test_restful_perf",
                                    "outputFields": ["id"],
                                    "filter": self.filter,
                                    "limit": 1000
                                    },
                              headers={"Content-Type": "application/json", "Authorization": "Bearer root:Milvus"},
                              catch_response=True
                              ) as resp:
            if resp.status_code != 200 or resp.json()["code"] != 0:
                resp.failure(f"query failed with error {resp.text}")
            else:
                pass
                # compute recall
                # result_ids = [item["id"] for item in resp.json()["data"]]
                # true_ids = [item for item in self.gt]
                # tmp = set(true_ids).intersection(set(result_ids))
                # self.recall = len(tmp) / len(result_ids) if len(result_ids) > 0 else 0
                # print(f"recall: {self.recall}")
                # self.recall_list.append(self.recall)
                # cur_time = datetime.now().timestamp()
                # self.ts_list.append(cur_time)

    # def on_stop(self):
    #     # this is a good place to clean up/release any user-specific test data
    #     # print(f"current recall is {self.recall}, "
    #     #       f"avg recall is {sum(self.recall_list) / len(self.recall_list)}, "
    #     #       f"max recall is {max(self.recall_list)}, "
    #     #       f"min recall is {min(self.recall_list)}")
    #     data = {"ts": self.ts_list, "recall": self.recall_list}
    #     df = pd.DataFrame(data)
    #     # print(df)

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
        {"duration": 120, "users": 200, "spawn_rate": 50},
        {"duration": 180, "users": 300, "spawn_rate": 50},
        {"duration": 240, "users": 400, "spawn_rate": 50},
        {"duration": 300, "users": 500, "spawn_rate": 50},
        {"duration": 360, "users": 600, "spawn_rate": 50, "stop": True},
    ]

    def tick(self):
        run_time = self.get_run_time()

        for stage in self.stages:
            if run_time < stage["duration"]:
                tick_data = (stage["users"], stage["spawn_rate"])
                return tick_data

        return None
