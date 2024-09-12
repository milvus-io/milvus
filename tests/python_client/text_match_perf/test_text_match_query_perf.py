from locust import HttpUser, task, events, LoadTestShape
import random
import pandas as pd
import re
from datetime import datetime
import uuid
from collections import Counter
import time
import bm25s
import jieba
def analyze_documents(texts, language="en"):
    # Create a stemmer
    # stemmer = Stemmer.Stemmer("english")
    stopwords = "en"
    if language in ["en", "english"]:
        stopwords= "en"
    if language in ["zh", "cn", "chinese"]:
        stopword = " "
        new_texts = []
        for doc in texts:
            seg_list = jieba.cut(doc)
            new_texts.append(" ".join(seg_list))
        texts = new_texts
        stopwords = [stopword]
    # Start timing
    t0 = time.time()

    # Tokenize the corpus
    tokenized = bm25s.tokenize(texts, lower=True, stopwords=stopwords)
    # log.info(f"Tokenized: {tokenized}")
    # Create a frequency counter
    freq = Counter()

    # Count the frequency of each token
    for doc_ids in tokenized.ids:
        freq.update(doc_ids)
    # Create a reverse vocabulary mapping
    id_to_word = {id: word for word, id in tokenized.vocab.items()}

    # Convert token ids back to words
    word_freq = Counter({id_to_word[token_id]: count for token_id, count in freq.items()})

    # End timing
    tt = time.time() - t0
    print(f"Analyze document cost time: {tt}")

    return word_freq, tokenized


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
    filter = "TextMatch(word, 'worker')"
    gt = []
    recall_list = []
    ts_list = []
    recall = 0


    @task
    def query(self):
        with self.client.post("/v2/vectordb/entities/query",
                              json={"collectionName": "test_text_match_perf",
                                    "outputFields": ["id"],
                                    "filter": self.filter,
                                    "limit": 1000
                                    },
                              headers={"Content-Type": "application/json", "Authorization": "Bearer root:Milvus"},
                              catch_response=True
                              ) as resp:
            if resp.status_code != 200 or resp.json()["code"] != 0:
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
