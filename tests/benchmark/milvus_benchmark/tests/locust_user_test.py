from pymilvus import DataType
from milvus_benchmark.runners.locust_user import locust_executor
from milvus_benchmark.client import MilvusClient


if __name__ == "__main__":
    connection_type = "single"
    host = "127.0.0.1"
    port = 19530
    collection_name = "sift_1m_128_l2"
    run_params = {"tasks": {"insert": 1}, "clients_num": 10, "spawn_rate": 2, "during_time": 3600}
    dim = 128
    m = MilvusClient(host=host, port=port, collection_name=collection_name)
    m.create_collection(dim, data_type=DataType.FLOAT_VECTOR, auto_id=False, other_fields=None)
    locust_executor(host, port, collection_name, run_params=run_params)
