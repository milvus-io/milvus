import time
import random
import threading
from concurrent.futures import ThreadPoolExecutor
import json
from datetime import datetime
import requests
from pymilvus import connections, Collection, DataType, FieldSchema, CollectionSchema, utility
from loguru import logger
import sys
logger.remove()
logger.add(sink=sys.stdout, level="INFO")
class MilvusCDCPerformance:
    def __init__(self, source_alias, target_alias, cdc_host):
        self.source_alias = source_alias
        self.target_alias = target_alias
        self.cdc_host = cdc_host
        self.source_collection = None
        self.target_collection = None
        self.insert_count = 0
        self.sync_count = 0
        self.insert_lock = threading.Lock()
        self.sync_lock = threading.Lock()
        self.latest_insert_ts = 0
        self.latest_query_ts = 0
        self.stop_query = False
        self.latencies = []
        self.latest_insert_status = {
            "latest_ts": 0,
            "latest_count": 0
        }

    def list_cdc_tasks(self):
        url = f"http://{self.cdc_host}:8444/cdc"
        payload = json.dumps({"request_type": "list"})
        response = requests.post(url, data=payload)
        result = response.json()
        logger.info(f"List CDC tasks response: {result}")
        return result["data"]["tasks"]

    def pause_cdc_tasks(self):
        tasks = self.list_cdc_tasks()
        for task in tasks:
            task_id = task["task_id"]
            url = f"http://{self.cdc_host}:8444/cdc"
            payload = json.dumps({
                "request_type": "pause",
                "request_data": {"task_id": task_id}
            })
            response = requests.post(url, data=payload)
            result = response.json()
            logger.info(f"Pause CDC task {task_id} response: {result}")
        self.cdc_paused = True
        logger.info("All CDC tasks paused")

    def resume_cdc_tasks(self):
        tasks = self.list_cdc_tasks()
        for task in tasks:
            task_id = task["task_id"]
            url = f"http://{self.cdc_host}:8444/cdc"
            payload = json.dumps({
                "request_type": "resume",
                "request_data": {"task_id": task_id}
            })
            response = requests.post(url, data=payload)
            result = response.json()
            logger.info(f"Resume CDC task {task_id} response: {result}")
        self.cdc_paused = False
        logger.info("All CDC tasks resumed")

    def pause_and_resume_cdc_tasks(self, duration):
        time.sleep(duration / 3)
        self.pause_cdc_tasks()
        time.sleep(duration / 3)
        self.resume_cdc_tasks()

    def setup_collections(self):
        self.resume_cdc_tasks()
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True, auto_id=True),
            FieldSchema(name="timestamp", dtype=DataType.INT64),
            FieldSchema(name="vector", dtype=DataType.FLOAT_VECTOR, dim=128)
        ]
        schema = CollectionSchema(fields, "Milvus CDC test collection")
        c_name = "milvus_cdc_perf_test" + datetime.now().strftime("%Y%m%d%H%M%S")
        # Create collections
        self.source_collection = Collection(c_name, schema, using=self.source_alias, num_shards=4)
        time.sleep(5)
        self.target_collection = Collection(c_name, using=self.target_alias)
        index_params = {
            "index_type": "IVF_FLAT",
            "metric_type": "L2",
            "params": {"nlist": 1024}
        }
        self.source_collection.create_index("vector", index_params)
        self.source_collection.load()
        time.sleep(1)
        logger.info(f"source collection: {self.source_collection.describe()}")
        logger.info(f"target collection: {self.target_collection.describe()}")

    def generate_data(self, num_entities):
        current_ts = int(time.time() * 1000)
        return [
            [current_ts for _ in range(num_entities)],  # timestamp
            [[random.random() for _ in range(128)] for _ in range(num_entities)]  # vector
        ]

    def continuous_insert(self, duration, batch_size):
        end_time = time.time() + duration
        while time.time() < end_time:
            entities = self.generate_data(batch_size)
            self.source_collection.insert(entities)
            with (self.insert_lock):
                self.insert_count += batch_size
                self.latest_insert_status = {
                    "latest_ts": entities[0][-1],
                    "latest_count": self.insert_count
                }  # Update the latest insert timestamp
                if random.random() < 0.1:
                    logger.debug(f"insert_count: {self.insert_count}, latest_ts: {self.latest_insert_status['latest_ts']}")
            time.sleep(0.01)  # Small delay to prevent overwhelming the system

    def continuous_query(self):
        while not self.stop_query:
            with self.insert_lock:
                latest_insert_ts = self.latest_insert_status["latest_ts"]
                latest_insert_count = self.latest_insert_status["latest_count"]
            if latest_insert_ts > self.latest_query_ts:
                try:
                    results = []
                    t0 = time.time()
                    while True and (time.time() - t0 < 10*60):
                        try:
                            results = self.target_collection.query(
                                expr=f"timestamp == {latest_insert_ts}",
                                output_fields=["timestamp"],
                                limit=1
                            )
                            break
                        except Exception as e:
                            logger.error(f"Query failed: {e}")
                            continue
                    tt = time.time() - t0
                    # logger.info(f"start to query, latest_insert_ts: {latest_insert_ts}, results: {results}")
                    if len(results) > 0 and results[0]["timestamp"] == latest_insert_ts:
                        end_time = time.time()
                        latency = end_time - (latest_insert_ts / 1000) - tt  # Convert milliseconds to seconds
                        with self.sync_lock:
                            self.latest_query_ts = latest_insert_ts
                            self.latencies.append(latency)
                        logger.debug(
                            f"query latest_insert_ts: {latest_insert_ts}, results: {results} query latency: {latency} seconds")
                except Exception as e:
                    logger.error(f"Query failed: {e}")
            time.sleep(0.01)  # Query interval

    def continuous_count(self):
        previous_count = self.target_collection.query(
                    expr="",
                    output_fields=["count(*)"],
                )[0]['count(*)']
        while not self.stop_query:
            try:
                t0 = time.time()
                results = self.target_collection.query(
                    expr="",
                    output_fields=["count(*)"],
                )
                tt = time.time() - t0
                count = results[0]['count(*)']
                with self.sync_lock:
                    self.sync_count = count - previous_count
                progress = (self.sync_count / self.insert_count) * 100 if self.insert_count > 0 else 0
                logger.debug(f"sync progress {self.sync_count}/{self.insert_count} {progress:.2f}%")
            except Exception as e:
                logger.error(f"Count failed: {e}")
            time.sleep(0.01)  # Query interval

    def measure_performance(self, duration, batch_size, concurrency):
        self.insert_count = 0
        self.sync_count = 0
        self.latest_insert_ts = 0
        self.latest_query_ts = int(time.time() * 1000)
        self.latencies = []
        self.stop_query = False

        start_time = time.time()

        # Start continuous query thread
        query_thread = threading.Thread(target=self.continuous_query)
        query_thread.start()
        count_thread = threading.Thread(target=self.continuous_count)
        count_thread.start()

        cdc_thread = threading.Thread(target=self.pause_and_resume_cdc_tasks, args=(duration,))
        cdc_thread.start()
        # Start continuous insert threads
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = [executor.submit(self.continuous_insert, duration, batch_size) for _ in range(concurrency)]

        # Wait for all insert operations to complete
        for future in futures:
            future.result()

        self.stop_query = True
        query_thread.join()
        count_thread.join()
        cdc_thread.join()

        end_time = time.time()
        total_time = end_time - start_time
        insert_throughput = self.insert_count / total_time
        sync_throughput = self.sync_count / total_time
        avg_latency = sum(self.latencies) / len(self.latencies) if self.latencies else 0

        logger.info(f"Test duration: {total_time:.2f} seconds")
        logger.info(f"Total inserted: {self.insert_count}")
        logger.info(f"Total synced: {self.sync_count}")
        logger.info(f"Insert throughput: {insert_throughput:.2f} entities/second")
        logger.info(f"Sync throughput: {sync_throughput:.2f} entities/second")
        logger.info(f"Average latency: {avg_latency:.2f} seconds")
        logger.info(f"Min latency: {min(self.latencies):.2f} seconds")
        logger.info(f"Max latency: {max(self.latencies):.2f} seconds")

        return total_time, self.insert_count, self.sync_count, insert_throughput, sync_throughput, avg_latency, min(
            self.latencies), max(self.latencies)

    def test_scalability(self, max_duration=600, batch_size=1000, max_concurrency=10):
        results = []
        for concurrency in range(10, max_concurrency + 1, 10):
            self.resume_cdc_tasks()
            logger.info(f"\nTesting with concurrency: {concurrency}")
            total_time, insert_count, sync_count, insert_throughput, sync_throughput, avg_latency, min_latency, max_latency = self.measure_performance(
                max_duration, batch_size, concurrency)
            results.append((concurrency, total_time, insert_count, sync_count, insert_throughput, sync_throughput,
                            avg_latency, min_latency, max_latency))

        logger.info("\nScalability Test Results:")
        for concurrency, total_time, insert_count, sync_count, insert_throughput, sync_throughput, avg_latency, min_latency, max_latency in results:
            logger.info(f"Concurrency: {concurrency}")
            logger.info(f"  Insert Throughput: {insert_throughput:.2f} entities/second")
            logger.info(f"  Sync Throughput: {sync_throughput:.2f} entities/second")
            logger.info(f"  Avg Latency: {avg_latency:.2f} seconds")

        return results

    def run_all_tests(self, duration=300, batch_size=1000, max_concurrency=10):
        logger.info("Starting Milvus CDC Performance Tests")
        self.setup_collections()
        self.test_scalability(duration, batch_size, max_concurrency)
        logger.info("Milvus CDC Performance Tests Completed")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='cdc perf test')
    parser.add_argument('--source_uri', type=str, default='http://10.104.14.232:19530', help='source uri')
    parser.add_argument('--source_token', type=str, default='root:Milvus', help='source token')
    parser.add_argument('--target_uri', type=str, default='http://10.104.34.103:19530', help='target uri')
    parser.add_argument('--target_token', type=str, default='root:Milvus', help='target token')
    parser.add_argument('--cdc_host', type=str, default='10.104.4.90', help='cdc host')

    args = parser.parse_args()

    connections.connect("source", uri=args.source_uri, token=args.source_token)
    connections.connect("target", uri=args.target_uri, token=args.target_token)
    cdc_test = MilvusCDCPerformance("source", "target", args.cdc_host)
    cdc_test.run_all_tests(duration=600, batch_size=1000, max_concurrency=20)
