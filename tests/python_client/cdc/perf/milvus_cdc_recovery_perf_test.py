import time
import random
import threading
from concurrent.futures import ThreadPoolExecutor
import json
from datetime import datetime
import requests
from pymilvus import connections, Collection, DataType, FieldSchema, CollectionSchema, utility
from loguru import logger
import matplotlib.pyplot as plt
import plotly.io as pio
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
import pandas as pd
import sys
logger.remove()
logger.add(sink=sys.stdout, level="DEBUG")


def create_interactive_plot(data, count_data):
    # Convert timestamps to datetime
    data['datetime'] = pd.to_datetime(data['timestamp'], unit='s')
    count_data['datetime'] = pd.to_datetime(count_data['timestamp'], unit='s')

    # Create a figure with subplots
    fig = make_subplots(rows=3, cols=1,
                        subplot_titles=("Source and Target Collection Count over Time",
                                        "Insert and Sync Throughput over Time",
                                        "Latency over Time"),
                        vertical_spacing=0.1)

    # Plot 1: Source and Target Collection Count
    fig.add_trace(go.Scatter(x=count_data['datetime'], y=count_data['source_count'],
                             mode='lines', name='Source Count'),
                  row=1, col=1)
    fig.add_trace(go.Scatter(x=count_data['datetime'], y=count_data['target_count'],
                             mode='lines', name='Target Count'),
                  row=1, col=1)

    # Plot 2: Insert and Sync Throughput
    fig.add_trace(go.Scatter(x=data['datetime'], y=data['insert_throughput'],
                             mode='lines', name='Insert Throughput'),
                  row=2, col=1)
    fig.add_trace(go.Scatter(x=data['datetime'], y=data['sync_throughput'],
                             mode='lines', name='Sync Throughput'),
                  row=2, col=1)

    # Plot 3: Latency
    fig.add_trace(go.Scatter(x=data['datetime'], y=data['avg_latency'],
                             mode='lines', name='Average Latency'),
                  row=3, col=1)
    fig.add_trace(go.Scatter(x=data['datetime'], y=data['real_time_latency'],
                             mode='lines', name='Real-time Latency'),
                  row=3, col=1)

    # Calculate and plot p99 latency
    p99_latency = [np.percentile(data['avg_latency'][:i + 1], 99) for i in range(len(data['avg_latency']))]
    fig.add_trace(go.Scatter(x=data['datetime'], y=p99_latency,
                             mode='lines', name='P99 Latency'),
                  row=3, col=1)

    # Update layout
    fig.update_layout(height=1200, width=1000, title_text="Milvus CDC Performance Metrics")

    # Update x-axes to show real datetime
    for i in range(1, 4):
        fig.update_xaxes(title_text="Time", row=i, col=1,
                         tickformat="%Y-%m-%d %H:%M:%S")

    fig.update_yaxes(title_text="Entity Count", row=1, col=1)
    fig.update_yaxes(title_text="Throughput (entities/second)", row=2, col=1)
    fig.update_yaxes(title_text="Latency (seconds)", row=3, col=1)

    return fig


class MilvusCDCPerformance:
    def __init__(self, source_alias, target_alias, cdc_host):
        self.source_alias = source_alias
        self.target_alias = target_alias
        self.cdc_host = cdc_host
        self.source_collection = None
        self.target_collection = None
        self.insert_count = 0
        self.sync_count = 0
        self.source_count = 0
        self.target_count = 0
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
        self.start_time = time.time()
        self.last_report_time = time.time()
        self.last_source_count = 0
        self.last_target_count = 0

        # New attributes for time series data
        self.time_series_data = {
            'timestamp': [],
            'insert_throughput': [],
            'sync_throughput': [],
            'avg_latency': [],
            'real_time_latency': []  # Add this line
        }
        self.count_series_data = {
            'timestamp': [],
            'source_count': [],
            'target_count': []
        }

    def report_realtime_metrics(self):
        current_time = time.time()
        if self.last_report_time is None:
            self.last_report_time = current_time
            self.last_source_count = self.source_count
            self.last_target_count = self.target_count
            return

        time_diff = current_time - self.last_report_time
        insert_diff = self.source_count - self.last_source_count
        sync_diff = self.target_count - self.last_target_count

        insert_throughput = insert_diff / time_diff
        sync_throughput = sync_diff / time_diff

        avg_latency = sum(self.latencies[-100:]) / len(self.latencies[-100:]) if self.latencies else 0

        logger.info(f"Real-time metrics:")
        logger.info(f"  Insert Throughput: {insert_throughput:.2f} entities/second")
        logger.info(f"  Sync Throughput: {sync_throughput:.2f} entities/second")
        logger.info(f"  Avg Latency (last 100): {avg_latency:.2f} seconds")

        # Store time series data
        self.time_series_data['timestamp'].append(current_time)
        self.time_series_data['insert_throughput'].append(insert_throughput)
        self.time_series_data['sync_throughput'].append(sync_throughput)
        self.time_series_data['avg_latency'].append(avg_latency)

        self.last_report_time = current_time
        self.last_source_count = self.source_count
        self.last_target_count = self.target_count

    def continuous_monitoring(self, interval=5):
        while not self.stop_query:
            real_time_latency = self.latencies[-1] if self.latencies else 0
            self.time_series_data['real_time_latency'].append(real_time_latency)
            self.report_realtime_metrics()

            time.sleep(interval)

    def plot_time_series_data(self):
        df = pd.DataFrame(self.time_series_data)
        count_df = pd.DataFrame(self.count_series_data)
        fig = create_interactive_plot(df, count_df)
        pio.write_html(fig, file='milvus_cdc_performance.html', auto_open=True)
        logger.info("Interactive performance plot saved as 'milvus_cdc_performance.html'")


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
                        except Exception as e:
                            logger.debug(f"Query failed: {e}")
                        logger.debug(
                            f"query latest_insert_ts: {latest_insert_ts}, results: {results} query latency: {time.time()-t0} seconds")
                        if len(results) > 0 and results[0]["timestamp"] == latest_insert_ts:
                            break
                    tt = time.time() - t0
                    # logger.info(f"start to query, latest_insert_ts: {latest_insert_ts}, results: {results}")
                    if len(results) > 0 and results[0]["timestamp"] == latest_insert_ts:
                        end_time = time.time()
                        latency = end_time - (latest_insert_ts / 1000)  # Convert milliseconds to seconds
                        with self.sync_lock:
                            self.latest_query_ts = latest_insert_ts
                            self.latencies.append(latency)
                        logger.debug(
                            f"query latest_insert_ts: {latest_insert_ts}, results: {results} query latency: {latency} seconds")
                except Exception as e:
                    logger.debug(f"Query failed: {e}")
            time.sleep(0.01)  # Query interval

    def continuous_count(self):

        def count_target():
            try:
                t0 = time.time()
                results = self.target_collection.query(
                    expr="",
                    output_fields=["count(*)"],
                    timeout=5
                )
                tt = time.time() - t0
                self.target_count = results[0]['count(*)']
            except Exception as e:
                logger.error(f"Target count failed: {e}")
                self.target_count = self.last_target_count

        def count_source():
            try:
                t0 = time.time()
                results = self.source_collection.query(
                    expr="",
                    output_fields=["count(*)"],
                    timeout=5
                )
                tt = time.time() - t0
                self.source_count = results[0]['count(*)']
            except Exception as e:
                logger.error(f"Source count failed: {e}")
                self.source_count = self.last_source_count
        previous_count = self.target_collection.query(
                    expr="",
                    output_fields=["count(*)"],
                )[0]['count(*)']
        while not self.stop_query:
            try:

                thread1 = threading.Thread(target=count_target)
                thread2 = threading.Thread(target=count_source)

                thread1.start()
                thread2.start()

                thread1.join()
                thread2.join()

                progress = (self.target_count / self.source_count) * 100 if self.source_count > 0 else 0

                self.sync_count = self.target_count - previous_count
                self.count_series_data['timestamp'].append(time.time())
                self.count_series_data['source_count'].append(self.source_count)
                self.count_series_data['target_count'].append(self.target_count)
                logger.debug(f"sync progress {self.target_count}/{self.source_count} {progress:.2f}%")
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
        monitor_thread = threading.Thread(target=self.continuous_monitoring)
        monitor_thread.start()

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
        monitor_thread.join()

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
        self.plot_time_series_data()
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
            logger.info(f"  Min Latency: {min_latency:.2f} seconds")
            logger.info(f"  Max Latency: {max_latency:.2f} seconds")

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
    cdc_test.run_all_tests(duration=20, batch_size=1000, max_concurrency=10)
