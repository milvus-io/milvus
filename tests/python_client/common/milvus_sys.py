import ujson
from pymilvus.grpc_gen import milvus_pb2 as milvus_types
from pymilvus import connections

sys_info_req = ujson.dumps({"metric_type": "system_info"})
sys_statistics_req = ujson.dumps({"metric_type": "system_statistics"})
sys_logs_req = ujson.dumps({"metric_type": "system_logs"})


class MilvusSys:
    def __init__(self, alias):
        self.alias = alias
        self.client = connections.get_connection(alias=self.alias)
        if self.client is None:
            raise Exception(f"Connection {alias} is disconnected or nonexistent")

        with self.client._connection() as handler:
            req = milvus_types.GetMetricsRequest(request=sys_info_req)
            self.sys_info = handler._stub.GetMetrics(req, wait_for_ready=True, timeout=None)
            req = milvus_types.GetMetricsRequest(request=sys_statistics_req)
            self.sys_statistics = handler._stub.GetMetrics(req, wait_for_ready=True, timeout=None)
            req = milvus_types.GetMetricsRequest(request=sys_logs_req)
            self.sys_logs = handler._stub.GetMetrics(req, wait_for_ready=True, timeout=None)


    @property
    def build_version(self):
        raise NotImplementedError()

    @property
    def deploy_mode(self):
        raise NotImplementedError()

    @property
    def simd_type(self):
        raise NotImplementedError()

    @property
    def query_nodes(self):
        raise NotImplementedError()

    @property
    def data_nodes(self):
        raise NotImplementedError()

    @property
    def index_nodes(self):
        raise NotImplementedError()

