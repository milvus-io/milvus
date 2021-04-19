from locust_user import locust_executor
from client import MilvusClient
from milvus import DataType


if __name__ == "__main__":
    connection_type = "single"
    host = "192.168.1.239"
    # host = "172.16.50.15"
    port = 19530
    collection_name = "sift_1m_2000000_128_l2_2"
    run_params = {"tasks": {"insert_rand": 5, "query": 10, "flush": 2}, "clients_num": 10, "spawn_rate": 2, "during_time": 3600}
    dim = 128
    m = MilvusClient(host=host, port=port, collection_name=collection_name)
    m.clean_db()
    m.create_collection(dim, data_type=DataType.FLOAT_VECTOR, auto_id=False, other_fields=None)

    locust_executor(host, port, collection_name, run_params=run_params)
