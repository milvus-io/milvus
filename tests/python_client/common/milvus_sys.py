import ujson
import json
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
        return self.nodes[0].get('infos').get('system_info').get('system_version')

    @property
    def deploy_mode(self):
        return self.nodes[0].get('infos').get('system_info').get('deploy_mode')

    @property
    def simd_type(self):
        raise NotImplementedError()
        # TODO: get simd_type when milvus metrics implemented
        # for node in self.nodes:
        #     if 'QueryNode' == node.get('infos').get('type'):
        #         return node.get('infos').get('simd_type')
        # raise Exception("No query node found")

    @property
    def query_nodes(self):
        raise NotImplementedError()

    @property
    def data_nodes(self):
        raise NotImplementedError()

    @property
    def index_nodes(self):
        raise NotImplementedError()

    @property
    def nodes(self):
        return json.loads(self.sys_info.response).get('nodes_info')

