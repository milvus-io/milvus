import time
import random
import threading
from concurrent.futures import ThreadPoolExecutor
from pymilvus import connections, Collection, DataType, FieldSchema, CollectionSchema
from loguru import logger
from kubernetes import client, config
from urllib.parse import urlparse
import itertools

class MilvusCDCPerformanceTest:
    def __init__(self, source_uri, source_token, target_uri, target_token, source_release_name):
        self.source_uri = source_uri
        self.source_token = source_token
        self.target_uri = target_uri
        self.target_token = target_token
        self.collection_name = "milvus_cdc_perf_test"
        self.insert_count = 0
        self.sync_count = 0
        self.insert_lock = threading.Lock()
        self.sync_lock = threading.Lock()
        self.stop_query = False
        self.latencies = []
        self.latest_insert_status = {"latest_ts": 0, "latest_count": 0}
        config.load_kube_config()

        self.k8s_api = client.CoreV1Api()
        self.source_release_name = source_release_name
        self.source_pod_ips = self.get_pod_ips(self.source_release_name)
        self.source_ip_cycle = itertools.cycle(self.source_pod_ips)
        self.init_collection()


    def init_collection(self):
        connections.connect(alias="default", uri=self.source_uri, token=self.source_token)
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True, auto_id=True),
            FieldSchema(name="timestamp", dtype=DataType.INT64),
            FieldSchema(name="vector", dtype=DataType.FLOAT_VECTOR, dim=128)
        ]
        schema = CollectionSchema(fields, "Milvus CDC test collection")
        collection = Collection(self.collection_name, schema, num_shards=8)
        index_params = {
            "index_type": "IVF_FLAT",
            "metric_type": "L2",
            "params": {"nlist": 1024}
        }
        collection.create_index("vector", index_params)
        collection.load()
        connections.disconnect(alias="default")
        return collection

    def get_pod_ips(self, instance, namespace='chaos-testing'):
        try:
            label_selector = f"app.kubernetes.io/instance={instance},app.kubernetes.io/component=proxy"
            pod_list = self.k8s_api.list_namespaced_pod(namespace=namespace, label_selector=label_selector)

            pod_ips = []
            for pod in pod_list.items:
                if pod.status.phase == 'Running' and pod.status.pod_ip:
                    pod_ips.append(pod.status.pod_ip)
            logger.info(f"Found {len(pod_ips)} pod IPs for instance {instance}: {pod_ips}")
            return pod_ips
        except Exception as e:
            logger.error(f"Error getting pod IPs: {str(e)}")
            return []
    def get_next_source_ip(self):
        return next(self.source_ip_cycle)

    def setup_collection(self, alias):
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True, auto_id=True),
            FieldSchema(name="timestamp", dtype=DataType.INT64),
            FieldSchema(name="vector", dtype=DataType.FLOAT_VECTOR, dim=128)
        ]
        schema = CollectionSchema(fields, "Milvus CDC test collection")
        collection = Collection(self.collection_name, schema, using=alias)
        return collection

    def generate_data(self, num_entities):
        current_ts = int(time.time() * 1000)
        return [
            [current_ts for _ in range(num_entities)],  # timestamp
            [[random.random() for _ in range(128)] for _ in range(num_entities)]  # vector
        ]

    def continuous_insert(self, duration, batch_size, thread_id):
        alias = f"source_{thread_id}"
        pod_ip = self.get_next_source_ip()
        uri = f"http://{pod_ip}:19530"
        connections.connect(alias, uri=uri, token=self.source_token)
        collection = self.setup_collection(alias)
        end_time = time.time() + duration
        local_insert_count = 0
        while time.time() < end_time:
            entities = self.generate_data(batch_size)
            collection.insert(entities)
            local_insert_count += batch_size
            with self.insert_lock:
                self.insert_count += batch_size
                self.latest_insert_status = {
                    "latest_ts": entities[0][-1],
                    "latest_count": self.insert_count
                }
            time.sleep(0.001)

        connections.disconnect(alias)
        logger.info(f"Thread {thread_id} inserted {local_insert_count} entities")

    def continuous_query(self):
        connections.connect("target", uri=self.target_uri, token=self.target_token)
        collection = self.setup_collection("target")

        latest_query_ts = int(time.time() * 1000)
        while not self.stop_query:
            with self.insert_lock:
                latest_insert_ts = self.latest_insert_status["latest_ts"]
                latest_insert_count = self.latest_insert_status["latest_count"]
            if latest_insert_ts > latest_query_ts:
                t0 = time.time()
                results = collection.query(
                    expr=f"timestamp == {latest_insert_ts}",
                    output_fields=["timestamp"],
                    limit=1
                )
                tt = time.time() - t0
                if len(results) > 0 and results[0]["timestamp"] == latest_insert_ts:
                    end_time = time.time()
                    latency = end_time - (latest_insert_ts / 1000) - tt
                    with self.sync_lock:
                        latest_query_ts = latest_insert_ts
                        self.sync_count = latest_insert_count
                        self.latencies.append(latency)
            time.sleep(0.01)

        connections.disconnect("target")

    def measure_performance(self, duration, batch_size, concurrency):
        self.insert_count = 0
        self.sync_count = 0
        self.latencies = []
        self.stop_query = False

        start_time = time.time()

        query_thread = threading.Thread(target=self.continuous_query)
        query_thread.start()

        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = [executor.submit(self.continuous_insert, duration, batch_size, i) for i in range(concurrency)]

        for future in futures:
            future.result()

        self.stop_query = True
        query_thread.join()

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

    def test_scalability(self, max_duration=300, batch_size=1000, max_concurrency=10):
        results = []
        for concurrency in range(10, max_concurrency + 1, 10):
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

        self.test_scalability(duration, batch_size, max_concurrency)
        logger.info("Milvus CDC Performance Tests Completed")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='cdc perf test')
    parser.add_argument('--source_uri', type=str, default='http://10.104.20.175:19530', help='source uri')
    parser.add_argument('--source_token', type=str, default='root:Milvus', help='source token')
    parser.add_argument('--target_uri', type=str, default='http://10.104.14.148:19530', help='target uri')
    parser.add_argument('--target_token', type=str, default='root:Milvus', help='target token')
    parser.add_argument('--source_release_name', type=str, default='cdc-test-upstream-12', help='source release name')
    args = parser.parse_args()
    cdc_test = MilvusCDCPerformanceTest(args.source_uri, args.source_token, args.target_uri, args.target_token, args.source_release_name)
    cdc_test.run_all_tests(duration=300, batch_size=1000, max_concurrency=100)
